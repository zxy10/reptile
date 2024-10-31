from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
from lxml import etree
import csv
import time
import pymysql


class Movie:
    def __init__(self):
        self.url = "https://movie.douban.com/top250?start={}&filter="
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        self.movies_data = []

        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "zxy110",
            "database": "mysql",
        }

    def parse_url(self, url):
        resp = requests.get(url, headers=self.headers)
        return resp.text


    def get_links(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        return [a['href'] for a in soup.select('li div a')]


    def parse_link(self, link):
        html = self.parse_url(link)
        hml = etree.HTML(html)
        # # //*[@id="content"]/div[1]/span[1]
        ranking = hml.xpath('//*[@id="content"]/div[1]/span[1]/text()')
        # //*[@id="content"]/h1/span[1]
        title = hml.xpath('//*[@id="content"]/h1/span[1]/text()')
        # //*[@id="info"]/span[1]/span[2]/a //*[@id="info"]/span[1]/span[2]/a
        director = hml.xpath('//*[@id="info"]/span[1]/span[2]/a/text()')
        # //*[@id="info"]/span[2]/span[2]
        scriptwriter = hml.xpath('//*[@id="info"]/span[2]/span[2]/a/text()')
        # //*[@id="info"]/span[3]/span[2]
        actors = hml.xpath('//*[@id="info"]/span[3]/span[2]//a/text()')
        # //*[@id="info"]/span[4]   //*[@id="info"]/span[5]   //*[@id="info"]/span[6]
        theme = hml.xpath('//*[@id="info"]/span[5]/text()')
        # //*[@id="info"]/text()[5]
        name = hml.xpath('//*[@class="pl" and text()="又名:"]/following-sibling::text()')
        # //*[@id="interest_sectl"]/div[1]/div[2]/strong
        score = hml.xpath('//*[@id="interest_sectl"]/div[1]/div[2]/strong/text()')
        # //*[@id="interest_sectl"]/div[1]/div[2]/div/div[2]/a
        score_num = hml.xpath('//*[@id="interest_sectl"]/div[1]/div[2]/div/div[2]/a/span/text()')
        # //*[@id="link-report-intra"]/span[1]/span //*[@id="link-report-intra"]/span[1]/span/text()[1]
        introduction = hml.xpath('//*[@property="v:summary"]/text()')
        introduction = ' '.join([text.strip() for text in introduction if text.strip()])

        self.movies_data.append({
            "排名": ranking[0],
            "标题": title[0],
            "导演": ', '.join(director),
            "编剧": ', '.join(scriptwriter),
            "演员": ', '.join(actors),
            "类型": ', '.join(theme),
            "又名": ', '.join(name).strip(),
            "评分": score[0].strip() if score else '',
            "评分人数": score_num[0].strip() if score_num else '',
            "简介": introduction
        })

    def create_table(self):
        conn = pymysql.connect(**self.db_config)  # 连接MySQL
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS movies (
                               ranking VARCHAR(255),  
                               title VARCHAR(255),        
                               director VARCHAR(255),           
                               scriptwriter VARCHAR(255),            
                               actor TEXT,     
                               theme VARCHAR(255),
                               name VARCHAR(255),
                               score VARCHAR(255),
                               score_num VARCHAR(255),
                               introduction TEXT            
                           )''')
        conn.commit()
        conn.close()

    # 向已创建的列表插入数据的方法
    def insert_data(self):
        conn = pymysql.connect(**self.db_config)
        cursor = conn.cursor()
        for movie_info in self.movies_data:
            cursor.execute('''INSERT INTO movies (ranking, title, director, scriptwriter, actor, theme, name, score, score_num, introduction)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                               (movie_info['排名'], movie_info['标题'], movie_info['导演'], movie_info['编剧'],
                                movie_info['演员'], movie_info['类型'], movie_info['又名'], movie_info['评分'],
                                movie_info['评分人数'], movie_info['简介']))
        conn.commit()
        conn.close()

    '''def write_to_csv(self, filename):
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.movies_data[0].keys())
            writer.writeheader()  # 写入表头
            writer.writerows(self.movies_data)  # 写入数据'''

    def run(self):
        with ThreadPoolExecutor(max_workers=10) as executor:  # 增加线程数
            for num in range(0, 250, 25):
                start_url = self.url.format(num)
                html = self.parse_url(start_url)
                if html:
                    links = self.get_links(html)
                    executor.map(self.parse_link, links)
                    time.sleep(1)  # 每个页面请求间隔1秒，减少请求频率

        '''self.write_to_csv("movies.csv")'''
        self.create_table()

        self.insert_data()


# 使用示例
if __name__ == "__main__":
    movie = Movie()
    movie.run()
