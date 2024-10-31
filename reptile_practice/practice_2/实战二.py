from selenium.webdriver import Edge
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import csv
import pymysql


web = Edge()
web.get('https://search.cnki.com.cn/Search/Result')

web.implicitly_wait(10)  # 可选，设置隐式等待


el = web.find_element(By.XPATH, '//*[@id="textSearchKey"]')
el.send_keys("机器学习", Keys.ENTER)

time.sleep(5)  # 等待页面加载

datas = []

conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="zxy110",
    port=3306
)
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS DATA CHARACTER SET utf8;")
cursor.execute("USE data;")
sql = """
CREATE TABLE datas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    introduce TEXT,
    writer VARCHAR(255),
    book VARCHAR(255),
    date VARCHAR(255),
    species VARCHAR(255),
    keyword VARCHAR(255),
    download_number VARCHAR(255),
    quote_number VARCHAR(255)
) DEFAULT CHARSET=utf8;
"""
cursor.execute(sql)

with open("datas.csv", mode="w", newline='', encoding='utf-8') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(["标题", "引言", "作者", "出版书", "日期", "文章属性", "关键词", "下载数量", "引用数量"])
    page = 1
    while page <= 10:
        time.sleep(3)
        div_list = web.find_elements(By.XPATH, '//*[@class="list-item"]')
        num = len(div_list)
        for i in range(1, num):
            # //*[@id="article_result"]/div/div[1]/p[1]/a[1]  //*[@id="article_result"]/div/div[2]/p[1]/a[1]
            title = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[1]/a[1]').text
            # //*[@id="article_result"]/div/div[2]/p[2]
            introduce = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[2]').text
            # //*[@id="article_result"]/div/div[1]/p[3]/span[1] //*[@id="article_result"]/div/div[2]/p[3]/span[1] //*[@id="article_result"]/div/div[2]/p[3]/span[1]
            writer = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[3]/span[1]').text
            # //*[@id="article_result"]/div/div[1]/p[3]/a[1]/span //*[@id="article_result"]/div/div[2]/p[3]/span[2] //*[@id="article_result"]/div/div[4]/p[3]/span[2]
            book = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[3]/a[1]/span | //*[@id="article_result"]/div/div[{i}]/p[3]/span[2]').text
            # //*[@id="article_result"]/div/div[1]/p[3]/a[2]/span //*[@id="article_result"]/div/div[2]/p[3]/span[3]
            date = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[3]/a[2]/span | //*[@id="article_result"]/div/div[{i}]/p[3]/span[3]').text
            # //*[@id="article_result"]/div/div[5]/p[3]/span[2] //*[@id="article_result"]/div/div[3]/p[3]/span[4]
            species = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/p[3]/span[2] | //*[@id="article_result"]/div/div[{i}]/p[3]/span[4]').text
            # //*[@id="article_result"]/div/div[5]/div[1]/p[1]  //*[@id="article_result"]/div/div[3]/div[1]/p[1]
            keyword = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/div[1]/p[1]').text
            # //*[@id="article_result"]/div/div[5]/div[1]/p[2]/span[1]
            download_number = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[{i}]/div[1]/p[2]/span[1]').text
            # //*[@id="article_result"]/div/div[6]/div[1]/p[2]/span[2]
            quote_number = web.find_element(By.XPATH, f'//*[@id="article_result"]/div/div[6]/div[1]/p[2]/span[2]').text

            datas.append({
                "标题": title,
                "引言": introduce,
                "作者": writer,
                "出版书": book,
                "日期": date,
                "文章属性": species,
                "关键词": keyword,
                "下载数量": download_number,
                "引用数量": quote_number
            })

            csvwriter.writerow([title, introduce, writer, book, date, species, keyword, download_number, quote_number])

            cursor.execute('''
                        INSERT INTO datas (title, introduce, writer, book, date, species, keyword, download_number, quote_number)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (title, introduce, writer, book, date, species, keyword, download_number, quote_number))
            conn.commit()


        page += 1
        # //*[@id="PageContent"]/div/div[1]/div[13]/a[2]/span
        button = web.find_element(By.XPATH, f'//*[@id="PageContent"]/div/div[1]/div[13]/a[{page}]/span').click()
        if page == 10:
            break

cursor.close()
conn.close()
web.quit()  # 关闭浏览器
