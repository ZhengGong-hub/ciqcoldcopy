import os 
import os.path as osp
import pandas as pd 

class S3FileHandler():
    @staticmethod
    def save_with_dir_create(path: str, ext: str, filename: str, file: any) -> bool:
        """_summary_

        Args:
            path (str): _description_
            ext (str): _description_
            filename (str): _description_
            file (any): _description_

        Raises:
            TypeError: _description_

        Returns:
            bool: Succeed True, failed False
        """
        os.makedirs(path, exist_ok=True)
        
        if ext.__contains__("txt"): # this way, more rebust and tolerant on "ext" var
            with open(osp.join(path, f'{filename}.txt'), 'w') as f:
                if type(file) is str:
                    f.write(file)
                    return True
                else:
                    raise TypeError('The file to save must be a str!')
                
        if ext.__contains__("csv"): # this way, more rebust and tolerant on "ext" var
            file.to_csv(osp.join(path, f'{filename}.csv'))
            return True

        if ext.__contains__("parquet"): # this way, more rebust and tolerant on "ext" var
            file.to_parquet(osp.join(path, f'{filename}.parquet'))
            return True
 
        return False

    @staticmethod
    def check_file_existence(path: str, ext: str, filename: str) -> bool:
        """_summary_

        Args:
            path (str): _description_
            ext (str): _description_
            filename (str): _description_

        Returns:
            bool: _description_
        """
        if ext.__contains__("txt"):
            file_path = osp.join(path, f'{filename}.txt')
            # print(file_path)
        
        if ext.__contains__("csv"):
            file_path = osp.join(path, f'{filename}.csv')
            # print(file_path)

        if ext.__contains__("parquet"):
            file_path = osp.join(path, f'{filename}.parquet')
                    
        if osp.isfile(file_path):
            return True 
        else:
            return False
        
    @staticmethod
    def get_file(path: str, ext: str, filename: str) -> bool:
        """_summary_

        Args:
            path (str): _description_
            ext (str): _description_
            filename (str): _description_

        Returns:
            bool: _description_
        """
        if ext.__contains__("txt"):
            file_path = osp.join(path, f'{filename}.txt')
            with open(file_path) as f:
                lines = f.readlines() # List 
            lines = ' '.join(lines)
            return lines

        if ext.__contains__("parquet"):
            file_path = osp.join(path, f'{filename}.parquet')
            return pd.read_parquet(file_path)
        
        return False