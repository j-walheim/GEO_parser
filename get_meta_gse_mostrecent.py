import ftplib
import os
import tarfile

def download_and_extract_miniml(ftp, subfolder):
    try:
        # Navigate to the subfolder
        ftp.cwd(f"/geo/series/GSE277nnn/{subfolder}")
        
        # Navigate to the miniml folder
        ftp.cwd("miniml")
        
        # Find the .tgz file
        tgz_files = [f for f in ftp.nlst() if f.endswith('.tgz')]
        if not tgz_files:
            print(f"No .tgz file found in {subfolder}/miniml")
            return
        
        tgz_file = tgz_files[0]
        
        # Download the .tgz file
        local_file = f"{subfolder}_miniml.tgz"
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {tgz_file}", f.write)
        
        # Create the data/GSE_meta directory if it doesn't exist
        os.makedirs("data/GSE_meta", exist_ok=True)
        
        # Extract the contents
        with tarfile.open(local_file, "r:gz") as tar:
            tar.extractall(path="data/GSE_meta")
        
        # Remove the .tgz file
        os.remove(local_file)
        
        # Delete non-XML files
        for root, dirs, files in os.walk("data/GSE_meta"):
            for file in files:
                if not file.endswith('.xml'):
                    os.remove(os.path.join(root, file))
        
        print(f"Successfully processed {subfolder}")
    except ftplib.error_perm as e:
        print(f"Error processing {subfolder}: {str(e)}")


def main():
    # Connect to the FTP server
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()

    try:
        # Navigate to the specified directory
        ftp.cwd("/geo/series/GSE277nnn")
        
        # List all subfolders
        subfolders = ftp.nlst()
        
        # Process each subfolder
        for subfolder in subfolders:
            download_and_extract_miniml(ftp, subfolder)
    
    finally:
        # Close the FTP connection
        ftp.quit()

if __name__ == "__main__":
    main()
