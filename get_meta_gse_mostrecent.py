import ftplib
import os
import tarfile
import re
import glob
import time
def get_most_recent_folders(ftp):
    ftp.cwd("/geo/series/")
    all_folders = ftp.nlst()
    pattern = re.compile(r'GSE(\d+)nnn')
    
    # Filter and sort folders
    valid_folders = [folder for folder in all_folders if pattern.match(folder)]
    sorted_folders = sorted(valid_folders, key=lambda x: int(pattern.match(x).group(1)), reverse=True)
    
    return sorted_folders


def download_and_extract_miniml(ftp_url, main_folder, subfolder):
    
#    time.sleep(0.1)
    ftp = ftplib.FTP(ftp_url)
    ftp.login()
    max_retries = 2
    retry_delay = 3
    for attempt in range(max_retries):

        try:
            # Navigate to the subfolder
            ftp.cwd(f"/geo/series/{main_folder}/{subfolder}")
            
            # Navigate to the miniml folder
            ftp.cwd("miniml")
            
            # Find the .tgz file
            tgz_files = [f for f in ftp.nlst() if f.endswith('.tgz')]
            if not tgz_files:
                print(f"No .tgz file found in {main_folder}/{subfolder}/miniml")
                return
            
            tgz_file = tgz_files[0]
            
            # Download the .tgz file
            local_file = f"{subfolder}_miniml.tgz"
            with open(local_file, 'wb') as f:
                ftp.retrbinary(f"RETR {tgz_file}", f.write)
            
            # Create the data/GSE_meta/{main_folder} directory if it doesn't exist
            os.makedirs(f"data/GSE_meta/{main_folder}", exist_ok=True)
            
            # Extract the contents
            with tarfile.open(local_file, "r:gz") as tar:
                tar.extractall(path=f"data/GSE_meta/{main_folder}")
            
            # Remove the .tgz file
            os.remove(local_file)
            
            # Delete non-XML files
            for root, dirs, files in os.walk(f"data/GSE_meta/{main_folder}"):
                for file in files:
                    if not file.endswith('.xml'):
                        os.remove(os.path.join(root, file))
            
            print(f"Successfully processed {main_folder}/{subfolder}")
            
            return
        except (ftplib.error_perm, EOFError) as e:
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        # Reconnect FTP
                        ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
                        ftp.login()
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed after {max_retries} attempts: {str(e)}")

def folder_already_processed(main_folder):
    folder_path = f"data/GSE_meta/{main_folder}"
    if os.path.exists(folder_path):
        xml_files = glob.glob(os.path.join(folder_path, "**", "*.xml"), recursive=True)
        return len(xml_files) > 0
    return False

def process_folder(ftp_url, folder):
    if folder_already_processed(folder):
        print(f"Folder {folder} has already been processed. Skipping.")
        return

    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()
    try:
        ftp.cwd(f"/geo/series/{folder}")
        subfolders = ftp.nlst()
        
    except ftplib.error_perm as e:
        print(f"Error accessing {folder}: {str(e)}")
        
    ftp.quit()
    
    for subfolder in subfolders:
        download_and_extract_miniml(ftp_url, folder, subfolder)

def main():
    # Connect to the FTP server
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()

    try:
        # Get the 40 most recent folders
        recent_folders = get_most_recent_folders(ftp)
    finally:
        # Close the FTP connection
        ftp.quit()
            
    # Process each folder
    for folder in recent_folders:
        process_folder(ftp_url="ftp.ncbi.nlm.nih.gov",folder=folder)



if __name__ == "__main__":
    main()
