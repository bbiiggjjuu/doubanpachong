import requests
import time
import random
import csv
import os
from tqdm import tqdm

def scrape_douban_chinese_movies(output_file='douban_movies.csv', start_page=1, end_page=50):
    """
    爬取豆瓣华语电影信息（支持分页范围）
    
    参数:
    output_file: 输出文件名
    start_page: 起始页码（包含）
    end_page: 结束页码（包含）
    """
    # 基础URL
    base_url = "https://movie.douban.com/j/new_search_subjects"
    
    # 请求头设置
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://movie.douban.com/',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # 查询参数模板
    params_template = {
        "sort": "U",  # 按热度排序
        "range": "0,10",
        "tags": "电影,法国",
        "start": 0  # 分页起始位置（将在循环中设置）
    }
    
    # 检查文件是否存在，决定写入模式
    file_exists = os.path.isfile(output_file)
    
    # 打开文件（追加模式）
    with open(output_file, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        
        # 如果是新文件，写入表头
        if not file_exists:
            writer.writerow(['标题', '评分', '年份', '导演', '演员', '详情链接', '封面链接'])
        
        # 分页爬取（使用tqdm显示进度条）
        for page in tqdm(range(start_page, end_page + 1), desc=f"爬取第 {start_page}-{end_page} 页"):
            # 计算起始位置（豆瓣每页20条数据）
            start_index = (page - 1) * 20
            params = params_template.copy()
            params["start"] = start_index
            
            try:
                # 发送请求
                response = requests.get(
                    url=base_url,
                    params=params,
                    headers=headers,
                    timeout=15
                )
                
                # 检查状态码
                if response.status_code != 200:
                    print(f"第 {page} 页请求失败，状态码: {response.status_code}")
                    time.sleep(10)  # 延长等待时间
                    continue
                    
                # 解析JSON数据
                data = response.json().get('data', [])
                if not data:
                    print(f"第 {page} 页没有数据，停止爬取")
                    break
                
                # 提取并写入数据
                for movie in data:
                    # 处理年份（只取前4位）
                    year = movie.get('date', '')[:4] if movie.get('date') else ''
                    
                    writer.writerow([
                        movie.get('title', ''),
                        movie.get('rate', 'N/A'),
                        year,
                        '/'.join(movie.get('directors', [])),
                        '/'.join(movie.get('casts', [])),
                        movie.get('url', ''),
                        movie.get('cover', '')
                    ])
                
                # 随机延时防止被封（2-5秒）
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                print(f"第 {page} 页出错: {str(e)}")
                time.sleep(10)  # 出错后延长等待

    print(f"爬取完成！第 {start_page}-{end_page} 页数据已保存到 {output_file}")

if __name__ == "__main__":
    # 示例：第一次运行爬取1-50页
    # scrape_douban_chinese_movies(
    #     output_file='华语电影.csv',
    #     start_page=1,
    #     end_page=50
    # )
    
    # 示例：第二次运行爬取51-100页
    scrape_douban_chinese_movies(
        output_file='./法国电影.csv',
        start_page=1,
        end_page=10
    )
    