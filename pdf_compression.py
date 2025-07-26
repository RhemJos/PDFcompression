# import os
# import sys
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil

LOG_FILE = "./failures.log"

def compress_pdf(input_path, output_path, target_size_mb=1.5, max_quality=40, min_quality=5):
    """
    Compress a PDF file to approximately target_size_mb using ocrmypdf with quality adjustments.
    """
    temp_output = output_path.with_suffix('.temp.pdf')

    best_quality_so_far = -1 # Initialize with a value that ensures first quality is better
    min_size_so_far = float('inf') # Initialize with infinity
    final_success = False # Tracks if target size was achieved
    current_quality = max_quality

    # quality = max_quality
    # success = False
    # best_attempt = None
    # best_temp_path = None # Stores the path to the best temporary file found so far
    
    while current_quality >= min_quality:
        try:
            # Run ocrmypdf with current quality setting
            cmd = [
                'ocrmypdf',
                # '--optimize', str(quality),
                '--optimize', '3',
                # '--skip-text',  # Skip OCR to save time if text already exists
                # '--force-ocr',  # Force OCR if needed (adjust based on your needs)
                # '--deskew',     # Deskew images
                # '--clean',     # Clean images
                # '--jbig2-lossy',  # Use lossy JBIG2 compression
                '--jpeg-quality', str(current_quality),
                str(input_path),
                str(temp_output)
            ]
            
            subprocess.run(cmd, check=True, capture_output=True)
            current_size = temp_output.stat().st_size
            
            if current_size <= target_size_mb * 1024 * 1024:
                shutil.move(temp_output, output_path)
                success = True
                break
            else:
                # if best_temp_path is None or current_size < best_temp_path.stat().st_size:
                #     if best_temp_path is not None:
                #         best_temp_path.unlink(missing_ok=True) 
                #     best_temp_path = temp_output.with_suffix('.best.pdf')
                #     shutil.move(temp_output, best_temp_path)
                # quality -= 5
                
                if current_size < min_size_so_far:
                    min_size_so_far = current_size
                    best_quality_so_far = current_quality
                if temp_output.exists():
                    temp_output.unlink(missing_ok=True)
                current_quality -= 5
                
        except subprocess.CalledProcessError as e:
            print(f"Error processing {input_path} with quality {current_quality}: {e.stderr.decode()}")
            # temp_output.unlink(missing_ok=True)
            # quality -= 5
            # continue
            if temp_output.exists():
                    temp_output.unlink(missing_ok=True)
            current_quality -= 5
            continue
        except FileNotFoundError:
            print(f"Error: ocrmypdf or tesseract not found. Please ensure they are installed and in your PATH.")
            return False
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return False
    
    # if not success:
    #     if best_temp_path is not None:
    #         shutil.move(best_temp_path, output_path)
    #         with open(LOG_FILE, 'a') as log:
    #             log.write(f"{input_path} - Compressed to {output_path.stat().st_size/1024/1024:.2f} MB (target: {target_size_mb} MB)\n")
    #         return True
    #     else:
    #         print(f"Error: Could not compress {input_path} at any quality level")
    #         return False
    
    # if temp_output.exists():
    #     temp_output.unlink(missing_ok=True)
    
    # return success
    
    if not final_success and best_quality_so_far != -1:
        try:
            print(f"Target size not met. Generating best possible output with quality {best_quality_so_far}.")
            cmd = [
                'ocrmypdf',
                '--optimize', '3',
                '--jpeg-quality', str(best_quality_so_far),
                str(input_path),
                str(output_path) # Write directly to final output path
            ]
            subprocess.run(cmd, check=True, capture_output=True)

            with open(LOG_FILE, 'a') as log:
                log.write(f"{input_path} - Compressed to {output_path.stat().st_size/1024/1024:.2f} MB (target: {target_size_mb} MB) (Best Effort)\n")
            return True # Indicate success for best effort
        except subprocess.CalledProcessError as e:
            print(f"Error generating best effort output for {input_path}: {e.stderr.decode()}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during best effort generation: {e}")
            return False
    elif not final_success and best_quality_so_far == -1:
        print(f"Error: Could not compress {input_path} at any quality level.")
        return False

    return final_success 


def process_pdf(input_path, output_path):
    """
    Process a single PDF file, creating parent directories if needed.
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            print(f"Skipping {input_path} - output already exists")
            return True
        
        print(f"Processing {input_path} -> {output_path}")
        return compress_pdf(input_path, output_path)
    
    except Exception as e:
        print(f"Error processing {input_path}: {str(e)}")
        return False

def find_and_compress_pdfs(source_dir, dest_dir, max_workers=4):
    """
    Find all PDFs in source_dir and compress them to dest_dir maintaining structure.
    """
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    if not source_path.exists():
        print(f"Error: Source directory {source_dir} does not exist")
        return
    
    pdf_files = list(source_path.glob('**/*.pdf'))
    total_files = len(pdf_files)
    print(f"Found {total_files} PDF files to process")
    
    # Parallel processing
    processed_count = 0
    failed_count = 0
    skipped_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for input_file in pdf_files:
            # Calculate corresponding output path
            relative_path = input_file.relative_to(source_path)
            output_file = dest_path / relative_path
            
            # Skip if output exists and is newer than input
            if output_file.exists() and output_file.stat().st_mtime >= input_file.stat().st_mtime:
                skipped_count += 1
                continue
                
            futures.append(executor.submit(process_pdf, input_file, output_file))
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    processed_count += 1
                else:
                    failed_count += 1
                
                if (processed_count + failed_count) % 100 == 0:
                    print(f"Progress: {processed_count + failed_count}/{total_files} processed")
                    
            except Exception as e:
                print(f"Error in future: {str(e)}")
                failed_count += 1
    
    print(f"\nProcessing complete:")
    print(f"- Total files: {total_files}")
    print(f"- Processed successfully: {processed_count}")
    print(f"- Failed: {failed_count}")
    print(f"- Skipped (already exists): {skipped_count}")

if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: python compress_pdfs.py <source_directory> <destination_directory>")
    #     sys.exit(1)
    
    # source_dir = sys.argv[1]
    # dest_dir = sys.argv[2]
    print("Executing...")

    source_dir = "J:\\OBSCD\\Python compression tests\\ballots\\d10\\b1\\000"
    dest_dir = "J:\\OBSCD\\Python compression test results"
    
    print(f"Starting PDF compression from {source_dir} to {dest_dir}")
    find_and_compress_pdfs(source_dir, dest_dir, max_workers=8)  # Adjust max_workers based on your CPU cores

