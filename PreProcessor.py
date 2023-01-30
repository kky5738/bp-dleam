import sys
import os
import pandas as pd

"""
    PATH1~5 is absolute path. Because, article data was saved in not current workspace
    
"""
PATH1 = "C:/bankrupt_prediction/data/0928_article_data/bankruptcy/KOSDAQ"
PATH2 = "C:/bankrupt_prediction/data/0928_article_data/bankruptcy/KOSPI"
PATH3 = "C:/bankrupt_prediction/data/0928_article_data/non_bankruptcy/KOSDAQ"
PATH4 = "C:/bankrupt_prediction/data/0928_article_data/non_bankruptcy/KOSPI"
PATH5 = "C:/bankrupt_prediction/data/0928_article_data/"
NAME_PATH = "./"

class PreProcessor():
    def __init__(self) -> None:
        pass

    def preprocess(self):
        # To-Do #
        # train에 쓰인 구글드라이브에 있는 데이터에서 회사 이름만 추출해서 기사 데이터에 있는 회사 이름과 비교하기
        
        f_name = ['bankruptcy_kosdaq_names.csv', 'bankruptcy_kospi_names.csv', 'non_bankruptcy_kosdaq_names.csv', 'non_bankruptcy_kospi_names.csv']
        d_path = ['bankruptcy/KOSDAQ', 'bankruptcy/KOSPI', 'non_bankruptcy/KOSDAQ', 'non_bankruptcy/KOSPI']
        
        unused, entire, used, before_2010 = self.find_unused(NAME_PATH, d_path, f_name)
        # find_unused 결과 확인
        # print(unused[0][:30])
        # for i in range(len(unused)):
        #     print(unused[i][:6], len(unused[i]))
        #     if i == 1: break
            # print(used[i], len(unused[i]))
            
            # print(f'data: {d_path[i]}, entire: {entire[i]}, used: {used[i]}')
            # print()
            
            
        

        # 안쓰인 회사 이름은 건너뛰고 기사 데이터 수 파악하는 부분 구현하기
        """
        unused와 상장폐지 일자가 필요함.
        unused라면 해당 회사 directory 삭제
        상장폐지 일자가 2010년 이전이라면 해당 회사 directory 삭제
        
        """
            
        # 아래 함수는 한번만 실행
        self.delete_file('1', '2', unused)
        
        pass

    def delete_file(self, path: str, file_name: str, unused_list: list):
        labelList = ["bankruptcy", "non_bankruptcy"]
        target_list = ['KOSDAQ', 'KOSPI']
        c = 0
        for i, (dirpath, dirnames, filenames) in enumerate(os.walk(PATH5)): # recursively go through all 
            if dirpath is not PATH5:
                
                dirpath_components = dirpath.split("\\") # bankruptcy/KOSDAQ => ["bankruptcy","KOSDAQ"]
                semantic_label = dirpath_components[-1] # list마지막-> "KOSDAQ"
                
                dirpath_part = dirpath_components[0].split('/')
                dirpath_part.append(semantic_label)
                for i in labelList:
                    if i in dirpath_part:
                        label_idx = labelList.index(i)
                        current_label = i
                # print("\nProcessing: {}, current_idx:{}" .format(semantic_label, i))

                # print(f'dirpath: {dirpath}, dirnames: {dirnames}, label_idx: {label_idx}, current_label: {current_label}')
                
                if semantic_label in target_list:                    
                    
                    unused_idx = int('0b' + str(labelList.index(dirpath_part[-2])) + str(target_list.index(semantic_label)), 2)
                    
                    for dir in dirnames:
                        if dir in unused_list[unused_idx]:
                            # now work is removing unused direcotry,  2023-01-30 save
                            # if os.path.isfile(path+file_name):
                            #     os.remove(path + file_name)
                            print(dir, 'was removed')
                            pass
                    c += 1
                    
                else:
                    continue
                if c == 2: exit()
      

    def find_unused(self, name_path: str, data_path: str, file_list) -> list:
        """Find unused company name using os.walk()

        Args:
            name_path (str): current directory
            data_path (str): news data directory's path
            file_list (_type_): name file path list

        Returns:
            list: unused name list
        """
        
        target_list = [] # list for save unused company name
        entire = []      # list for save entire number of company name
        used = []        # list for save usedly used company name in model train
        before_2010 = [] # list for save company name that bankrupted before 2010
        
        for idx, file_path in enumerate(file_list):
        
            name_list = self.make_name_df(name_path, file_path)
            used_name = list(name_list.loc[:, 'names'])
            
            temp = []
            act = len(name_list.iloc[:, 0])
            
            # 네이버 뉴스가 서비스를 시작한 년도 2010년보다 이전에 상장폐지한 기업 이름 리스트
            before_news = list(name_list[name_list['bankrupted_date'] < 20100101].loc[:, 'names']) 
            before_2010.append(before_news)
            
            for (dirpath, dirnames, filenames) in os.walk(PATH5 + data_path[idx]): # recursively go through all 
                ent = len(dirnames)
                for company in dirnames:
                    f = False
                    if '%26' in company: 
                        company = company.replace('%26', '&')
                        # print(company)
                        f = True
                    if (company not in used_name) or (company in before_news):
                        if f == True:
                            company = company.replace('&', '%26')
                            temp.append(company)
                        else:
                            temp.append(company)
                    
                break
            target_list.append(temp)
            entire.append(ent)
            used.append(act)
            
        return target_list, entire, used, before_2010
        
    def make_name_df(self, path1:str, path2: str):
        df = pd.read_csv(path1 + path2)
        
        return df.sort_values('names')

if __name__ == '__main__':
    p = PreProcessor()
    p.preprocess()