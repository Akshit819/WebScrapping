import io
import fitz  # PyMuPDF
import pandas as pd
import re
from collections import defaultdict
from google.cloud import vision
import os
import numpy as np
import pdfplumber
from PIL import Image, ImageDraw, ImageOps, ImageFilter

# ==============================================================================
# PART 1: THE PDF ORCHESTRATOR WITH RECTANGLE DETECTION
# ==============================================================================

def preprocess_image_bytes(image_bytes):
    img = Image.open(io.BytesIO(image_bytes)).convert("L")
    img = ImageOps.autocontrast(img)
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = ImageOps.invert(img)

    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()

def detect_largest_table_rectangle(page):
    try:
        tables = page.find_tables()
        if tables:
            largest_table = max(tables, key=lambda t: t.bbox.width * t.bbox.height)
            return largest_table.bbox
    except:
        pass

    drawings = page.get_drawings()
    text_blocks = page.get_text("dict")["blocks"]
    rectangles = []

    for drawing in drawings:
        for item in drawing["items"]:
            if item[0] == "re":
                x0, y0, x1, y1 = item[1]
                width = abs(x1 - x0)
                height = abs(y1 - y0)
                if width > 100 and height > 50:
                    rectangles.append(fitz.Rect(x0, y0, x1, y1))

    if not rectangles and text_blocks:
        numeric_blocks = []
        for block in text_blocks:
            if "lines" in block:
                for line in block["lines"]:
                    text = "".join(span["text"] for span in line["spans"])
                    if re.search(r'\d+[,.]?\d*', text):
                        numeric_blocks.append(block["bbox"])

        if numeric_blocks:
            min_x = min(bbox[0] for bbox in numeric_blocks)
            min_y = min(bbox[1] for bbox in numeric_blocks)
            max_x = max(bbox[2] for bbox in numeric_blocks)
            max_y = max(bbox[3] for bbox in numeric_blocks)
            padding = 20
            page_rect = page.rect
            table_rect = fitz.Rect(
                max(min_x - padding, 0),
                max(min_y - padding, 0),
                min(max_x + padding, page_rect.width),
                min(max_y + padding, page_rect.height)
            )
            rectangles.append(table_rect)

    if rectangles:
        return max(rectangles, key=lambda r: r.width * r.height)
    return page.rect

def crop_page_to_rectangle(page, rect):
    page.set_cropbox(rect)
    mat = fitz.Matrix(3.0, 3.0)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    page.set_cropbox(page.rect)
    return img_bytes

def page_has_table_and_keyword(page, keyword):
    text = page.get_text().lower()
    if keyword.lower() not in text:
        return False

    table_indicators = [
        len(re.findall(r'\d+[,.]?\d*', text)) > 10,
        len(re.findall(r'\$\s*\d+', text)) > 3,
        len(re.findall(r'\d+%', text)) > 2,
        'total' in text and 'amount' in text,
        text.count('\n') > 10 and len(text.split()) > 50
    ]
    return sum(table_indicators) >= 2

def try_pdfplumber_fallback(pdf_path, page_num):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_num]
            if "consolidated" in page.extract_text().lower():
                tables = page.extract_tables()
                if tables:
                    return pd.DataFrame(tables[0])
    except Exception as e:
        print(f"  pdfplumber fallback failed: {e}")
    return pd.DataFrame()

def process_pdf_for_tables(pdf_path, output_excel_path, keyword="consolidated"):
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at '{pdf_path}'")
        return

    print(f"Processing PDF: {pdf_path}")
    doc = fitz.open(pdf_path)
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        extracted_tables_count = 0
        for page_num, page in enumerate(doc):
            print(f"\n--- Analyzing Page {page_num + 1}/{len(doc)} ---")
            if not page_has_table_and_keyword(page, keyword):
                print(f"Skipping Page {page_num + 1}: Does not meet criteria.")
                continue

            print(f"Page {page_num + 1} is a candidate. Detecting table rectangle...")
            try:
                table_rect = detect_largest_table_rectangle(page)
                img_bytes = crop_page_to_rectangle(page, table_rect)
                img_bytes = preprocess_image_bytes(img_bytes)
                with open(f"debug_page_{page_num + 1}.png", "wb") as f:
                    f.write(img_bytes)
                df = extract_table_from_image_data(img_bytes)

                if not df.empty:
                    sheet_name = f'Page_{page_num + 1}_Table'
                    df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                    print(f"SUCCESS: Extracted table from Page {page_num + 1}")
                    extracted_tables_count += 1
                else:
                    print(f"NOTE: No table from OCR. Trying pdfplumber fallback on Page {page_num + 1}...")
                    fallback_df = try_pdfplumber_fallback(pdf_path, page_num)
                    if not fallback_df.empty:
                        sheet_name = f'Fallback_{page_num + 1}'
                        fallback_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                        print(f"SUCCESS: Fallback worked for Page {page_num + 1}")
                        extracted_tables_count += 1
                    else:
                        print(f"FAILURE: No table found from OCR or fallback on Page {page_num + 1}")

            except Exception as e:
                print(f"ERROR on Page {page_num + 1}: {e}")

    doc.close()
    print(f"\nProcessing complete. Extracted {extracted_tables_count} table(s). Output saved to: {output_excel_path}")

# ==============================================================================
# PART 2: OCR + TABLE RECONSTRUCTION
# ==============================================================================

def extract_table_from_image_data(image_bytes):
    try:
        api_response = get_full_ocr_response(image_bytes)
        all_words = get_all_words(api_response)
        if not all_words:
            print("  No words detected in the image.")
            return pd.DataFrame()
        lines = reconstruct_lines_intelligently(all_words)
        if not lines:
            print("  No lines could be reconstructed.")
            return pd.DataFrame()
        boundaries = detect_boundaries_by_gaps(lines)
        table_grid = build_table_by_spacing(lines)
        return pd.DataFrame(table_grid)
    except Exception as e:
        print(f"  Error in table extraction: {e}")
        return pd.DataFrame()

def get_full_ocr_response(image_bytes):
    print("  Calling Google Cloud Vision API on focused region...")
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_bytes)
    features = [vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)]
    request = vision.AnnotateImageRequest(image=image, features=features)
    response = client.annotate_image(request)
    if response.error.message:
        raise Exception(f'Google Vision API Error: {response.error.message}')
    return response

def get_all_words(response):
    all_words = []
    document = response.full_text_annotation
    if not document:
        return all_words
    for page in document.pages:
        for block in page.blocks:
            for paragraph in block.paragraphs:
                for word in paragraph.words:
                    word_text = ''.join([symbol.text for symbol in word.symbols])
                    if len(word_text.strip()) > 0:
                        vertices = word.bounding_box.vertices
                        all_words.append({'text': word_text, 'vertices': vertices})
    return all_words

def is_numeric_like(text):
    text = text.strip()
    if not text:
        return False
    numeric_patterns = [
        r'^-?[\d,]+\.?\d*$',
        r'^-?\$[\d,]+\.?\d*$',
        r'^-?[\d,]+\.?\d*%$',
        r'^\([\d,]+\.?\d*\)$'
    ]
    return any(re.match(pattern, text) for pattern in numeric_patterns) or text in ['(', ')']

def reconstruct_lines_intelligently(all_words):
    print("  Reconstructing table rows...")
    if not all_words:
        return []
    all_words.sort(key=lambda w: (w['vertices'][0].y, w['vertices'][0].x))
    lines = []
    current_line = [all_words[0]]
    for i in range(1, len(all_words)):
        prev_word = current_line[-1]
        current_word = all_words[i]
        prev_y = (prev_word['vertices'][0].y + prev_word['vertices'][2].y) / 2
        curr_y = (current_word['vertices'][0].y + current_word['vertices'][2].y) / 2
        vertical_gap = abs(curr_y - prev_y)
        if vertical_gap > 10:
            lines.append(current_line)
            current_line = [current_word]
        else:
            current_line.append(current_word)
    if current_line:
        lines.append(current_line)
    return lines

def detect_boundaries_by_gaps(lines):
    print("  Detecting column boundaries...")
    all_x_positions = []
    for line in lines:
        for word in line:
            if is_numeric_like(word['text']) or len(word['text']) > 2:
                x_start = word['vertices'][0].x
                x_end = word['vertices'][1].x
                all_x_positions.extend([x_start, x_end])
    if not all_x_positions:
        return [0]
    all_x_positions.sort()
    boundaries = [0]
    for i in range(1, len(all_x_positions)):
        gap = all_x_positions[i] - all_x_positions[i - 1]
        if gap > 30:
            mid_point = (all_x_positions[i - 1] + all_x_positions[i]) / 2
            if not any(abs(mid_point - b) < 20 for b in boundaries):
                boundaries.append(mid_point)
    boundaries.sort()
    return boundaries

def filter_out_below_large_gap(lines, gap_threshold=50):
    """
    Remove lines after a large vertical gap which usually separates table and notes.
    """
    if len(lines) < 2:
        return lines

    last_valid_index = len(lines) - 1
    for i in range(1, len(lines)):
        prev_line_y = np.mean([w['vertices'][0].y for w in lines[i - 1]])
        curr_line_y = np.mean([w['vertices'][0].y for w in lines[i]])
        gap = curr_line_y - prev_line_y
        if gap > gap_threshold:
            last_valid_index = i
            break

    return lines[:last_valid_index]

def build_table_by_spacing(lines, max_col_gap=30):
    """
    Builds a table where columns are separated based on horizontal gaps within a line.
    More flexible than fixed boundaries.
    """
    print("  Building final table using adaptive spacing...")
    table = []

    for line in lines:
        line.sort(key=lambda w: w['vertices'][0].x)

        row = []
        current_cell = line[0]['text']
        last_x = line[0]['vertices'][1].x  # end of first word

        for word in line[1:]:
            curr_x = word['vertices'][0].x
            gap = curr_x - last_x

            if gap > max_col_gap:
                row.append(current_cell.strip())
                current_cell = word['text']
            else:
                current_cell += ' ' + word['text']

            last_x = word['vertices'][1].x

        row.append(current_cell.strip())
        table.append(row)

    return table

# ==============================================================================
# PART 3: SCRIPT ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    pdf_to_process = "trial_pdf.pdf"
    output_excel_file = "extracted_financial_trial2.xlsx"
    process_pdf_for_tables(pdf_to_process, output_excel_file)