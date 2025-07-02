import requests
from bs4 import BeautifulSoup
import time
import random
import csv
import os
import re


# 使用新代理
PROXY = {
    "http": "http://114.80.161.92:55005",
    "https": "http://114.80.161.92:55005"
}
PROXY = None
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.2151.97',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
]

REFERERS = [
    'https://www.google.com/',
    'https://www.baidu.com/'
]


def get_default_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': random.choice(REFERERS),
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }


def wait_for_verification():
    print("\n 检测到403错误或验证码页面，请在浏览器中手动访问最后一个URL进行验证。")
    input("请完成验证后按下回车键继续采集...")


def get_movie_id(url):
    from urllib.parse import urlparse
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')
    return path_parts[1] if len(path_parts) >= 2 and path_parts[0] == 'subject' else ''


def fetch_short_comments(movie_id, headers):
    """获取豆瓣短评（最多10条）"""
    url_base = f'https://movie.douban.com/subject/{movie_id}/comments?start='
    session = requests.Session()
    session.headers.update(headers)

    short_comments = []
    error_count = 0  # 错误计数器
    max_errors = 5  # 最大允许错误次数

    start = 0
    while len(short_comments) < 10:
        try:
            url = url_base + str(start)
            print(f"正在抓取短评页: {url}")
            response = session.get(url, timeout=(30, 60), proxies=PROXY)

            if response.status_code == 403:
                print("服务器返回403 Forbidden，可能需要验证码验证。")
                wait_for_verification()
                continue

            if response.status_code != 200:
                raise Exception(f"HTTP状态码异常: {response.status_code}")

            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_='comment-item')

            if not items:
                print("未找到评论项，可能是反爬或页面结构变化")
                error_count += 1
                if error_count >= max_errors:
                    print("错误次数过多，跳过该电影")
                    return []  # 返回空列表，外层判断后跳过保存
                continue

            parsed_any = False  # 是否成功解析至少一条评论
            for item in items:
                if len(short_comments) >= 10:
                    break

                user_info = item.find('span', class_='comment-info')
                if not user_info:
                    print("未找到 comment-info，跳过该评论")
                    error_count += 1
                    if error_count >= max_errors:
                        print("错误次数过多，跳过该电影")
                        return []
                    continue

                user = user_info.find('a')
                if not user:
                    print("未找到用户名，跳过该评论")
                    error_count += 1
                    if error_count >= max_errors:
                        print("错误次数过多，跳过该电影")
                        return []
                    continue
                user = user.get_text(strip=True)

                date = item.find('span', class_='comment-time')
                if not date or 'title' not in date.attrs:
                    print("未找到日期或格式不正确，跳过该评论")
                    error_count += 1
                    if error_count >= max_errors:
                        print("错误次数过多，跳过该电影")
                        return []
                    continue
                date = date['title']

                content = item.find('span', class_='short')
                if not content:
                    print("未找到评论内容，跳过该评论")
                    error_count += 1
                    if error_count >= max_errors:
                        print("错误次数过多，跳过该电影")
                        return []
                    continue
                content = content.get_text(strip=True)

                votes = item.find('span', class_='votes')
                if not votes:
                    print("未找到有用数，跳过该评论")
                    error_count += 1
                    if error_count >= max_errors:
                        print("错误次数过多，跳过该电影")
                        return []

                votes = votes.get_text(strip=True)

                short_comments.append([user, date, content, votes])
                error_count = 0  # 成功解析一条评论，清零错误计数器
                parsed_any = True

            if not parsed_any:
                # 当前页没有成功解析任何评论
                error_count += 1
                if error_count >= max_errors:
                    print("连续多页无有效评论，跳过该电影")
                    return []

            start += 20
            time.sleep(random.uniform(2, 5))

        except requests.exceptions.ProxyError as pe:
            print(f"代理错误: {pe}，请检查代理是否可用")
            input("请确认代理后按回车继续...")
            continue

        except Exception as e:
            print(f"获取短评失败，跳过该电影: {e}")
            return []

    return short_comments


def fetch_movie_info(movie_id, headers):
    url = f'https://movie.douban.com/subject/{movie_id}/'
    session = requests.Session()
    session.headers.update(headers)

    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            print(f"\n【尝试 {retry_count + 1}/{max_retries}】正在抓取电影信息: {url}")
            response = session.get(url, timeout=(30, 60), proxies=PROXY)

            if response.status_code == 403:
                print("服务器返回403 Forbidden，可能需要验证码验证。")
                wait_for_verification()
                continue

            if response.status_code != 200:
                raise Exception(f"HTTP状态码异常: {response.status_code}")

            if len(response.content) < 10_000:
                print("响应内容太小，疑似被反爬或返回了验证码页面")
                retry_count += 1
                time.sleep(random.uniform(15, 30))
                continue

            if "验证码" in response.text or "security" in response.text:
                print("检测到验证码页面，请手动访问进行验证。")
                wait_for_verification()
                continue

            # 解析 HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            if not soup:
                print("soup 解析失败，可能是无效HTML内容")
                retry_count += 1
                time.sleep(random.uniform(15, 30))
                continue

            # 关键判断：找不到 info 区域就直接跳过整部电影
            info_div = soup.find('div', id='info')
            if not info_div:
                print("未找到电影信息区域，可能是页面结构变化，直接跳过该电影")
                return None  # 外层函数将跳过这部影片

            # 保存 debug HTML 内容
            with open("debug_info_div.html", "w", encoding="utf-8") as f:
                f.write(str(info_div))

            def get_text(selector):
                el = soup.select_one(selector)
                return el.get_text(strip=True) if el else ''

            # 提取导演
            director = get_text('span:nth-child(1) > span.attrs a') or '未知'

            # 提取编剧
            writer = ''
            for i in range(2, 5):  # 尝试第2~4个 span.attrs 元素
                writer = get_text(f'span:nth-child({i}) > span.attrs')
                if writer:
                    break
            if not writer:
                match = re.search(r'编剧[:：]\s*([^<]+)', str(info_div))
                writer = match.group(1).strip() if match else '未知'

            # 提取主演
            actors = []
            actor_span = info_div.find('span', string=re.compile('主演'))
            if actor_span:
                next_node = actor_span.next_sibling
                while next_node and not isinstance(next_node, str):
                    next_node = next_node.next_sibling
                if next_node:
                    raw_actors = next_node.strip().split('/')
                    actors = [a.strip() for a in raw_actors if a.strip()]
            actor_str = ' / '.join(actors) if actors else '未知'

            # 提取类型
            genres = [a.text for a in info_div.select('span[property="v:genre"]')]
            genre_str = ' / '.join(genres) if genres else '未知'

            # 提取制片国家
            region = ''
            region_match = re.search(r'制片国家[/／]地区[:：]\s*([^<>\n]+)', str(info_div))
            if region_match:
                region = region_match.group(1).strip()
            else:
                region_span = info_div.find('span', string=re.compile('制片国家'))
                if region_span:
                    next_node = region_span.next_sibling
                    while next_node and not isinstance(next_node, str):
                        next_node = next_node.next_sibling
                    region = next_node.strip() if next_node else '未知'
            region = region or '未知'

            # 提取语言
            language = ''
            lang_match = re.search(r'语言[:：]\s*([^<>\n]+)', str(info_div))
            if lang_match:
                language = lang_match.group(1).strip()
            else:
                lang_span = info_div.find('span', string=re.compile('语言'))
                if lang_span:
                    next_node = lang_span.next_sibling
                    while next_node and not isinstance(next_node, str):
                        next_node = next_node.next_sibling
                    language = next_node.strip() if next_node else '未知'
            language = language or '未知'

            # 提取上映日期
            release_date = ''
            date_match = re.search(r'上映日期[:：]\s*([^<>\n]+)', str(info_div))
            if date_match:
                release_date = date_match.group(1).strip()
            else:
                date_span = info_div.find('span', string=re.compile('上映日期'))
                if date_span:
                    next_node = date_span.next_sibling
                    while next_node and not isinstance(next_node, str):
                        next_node = next_node.next_sibling
                    release_date = next_node.strip() if next_node else '未知'
            release_date = release_date or '未知'

            # 提取片长
            runtime = ''
            runtime_match = re.search(r'片长[:：]\s*([^<>\n]+)', str(info_div))
            if runtime_match:
                runtime = runtime_match.group(1).strip()
            else:
                runtime_span = info_div.find('span', string=re.compile('片长'))
                if runtime_span:
                    next_node = runtime_span.next_sibling
                    while next_node and not isinstance(next_node, str):
                        next_node = next_node.next_sibling
                    runtime = next_node.strip() if next_node else '未知'
            runtime = runtime or '未知'

            # 提取IMDb链接
            imdb_link = ''
            imdb_a = info_div.find('a', href=re.compile(r'^https?://www\.imdb\.com/title/', re.I))
            if imdb_a:
                imdb_link = imdb_a['href']
            else:
                imdb_match = re.search(r'IMDb[:：]\s*([^\s<]+)', str(info_div))
                imdb_link = imdb_match.group(1).strip() if imdb_match else '未知'

            # 提取又名
            aka = ''
            aka_match = re.search(r'又名[:：]\s*([^/]+)', str(info_div))
            if aka_match:
                aka = aka_match.group(1).strip()
            else:
                aka = get_text('span[property="v:alsoKnownAs"]') or '未知'

            # 提取评分
            rating = '暂无评分'
            rating_elem = soup.select_one('strong.rating_num')
            if rating_elem:
                rating = rating_elem.text.strip()

            # 提取评价人数
            rating_people = '0人'
            people_elem = soup.select_one('.rating_people span')
            if people_elem:
                rating_people = people_elem.text.strip() + '人'

            # 获取电影名称
            title_element = soup.find('h1')
            title = title_element.find('span').text.strip() if title_element and title_element.find('span') else '未知'

            # 获取剧情简介
            summary_element = soup.find('span', property='v:summary')
            summary = summary_element.text.strip() if summary_element else '未知'

            # 获取短评
            short_comments = fetch_short_comments(movie_id, headers)

            data = {
                '电影名称': title,
                '导演': director,
                '编剧': writer,
                '主演': actor_str,
                '类型': genre_str,
                '制片国家': region,
                '语言': language,
                '上映时间': release_date,
                '片长': runtime,
                'IMDB链接': imdb_link,
                '又名': aka,
                '评分': rating,
                '评价人数': rating_people,
                '剧情简介': summary,
                '短评': short_comments
            }

            output_dir = os.path.join(os.path.dirname(__file__), 'result')  # 相对路径 networkhomework\result
            print(f"尝试保存到目录: {output_dir}")

            try:
                os.makedirs(output_dir, exist_ok=True)
                path = os.path.join(output_dir, f"{movie_id}.txt")
                with open(path, 'w', encoding='utf-8') as f:
                    f.write("基本信息\n")
                    f.write(f"电影名称: {data['电影名称']}\n")
                    f.write(f"导演: {data['导演']}\n")
                    f.write(f"编剧: {data['编剧']}\n")
                    f.write(f"主演: {data['主演']}\n")
                    f.write(f"类型: {data['类型']}\n")
                    f.write(f"制片国家: {data['制片国家']}\n")
                    f.write(f"语言: {data['语言']}\n")
                    f.write(f"上映时间: {data['上映时间']}\n")
                    f.write(f"片长: {data['片长']}\n")
                    f.write(f"IMDB链接: {data['IMDB链接']}\n")
                    f.write(f"又名: {data['又名']}\n")
                    f.write(f"评分: {data['评分']}\n")
                    f.write(f"评价人数: {data['评价人数']}\n\n")

                    f.write("剧情简介\n")
                    f.write(f"{data['剧情简介']}\n\n")

                    f.write("短评\n")
                    if data['短评']:
                        for c in data['短评']:
                            f.write(f"用户名: {c[0]}\n")
                            f.write(f"日期: {c[1]}\n")
                            f.write(f"内容: {c[2]}\n")
                            f.write(f"有用数: {c[3]}\n")
                            f.write("\n")
                    else:
                        f.write("暂无短评\n")

                print(f"\n 已成功保存至: {path}")
            except Exception as e:
                print(f" 保存文件失败，可能是路径不可写或权限不足: {e}")

            return data

        except requests.exceptions.ProxyError as pe:
            print(f"代理错误: {pe}，请检查代理是否可用")
            input("请确认代理后按回车继续...")
            retry_count += 1
            continue

        except Exception as e:
            print(f"获取电影信息失败 [{retry_count+1}/{max_retries}]: {e}")
            retry_count += 1
            time.sleep(random.uniform(15, 30))

    return None


def scrape_all_movies_from_csv(csv_path, start_index):
    print(f"正在读取CSV文件: {csv_path}")

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            if not rows:
                print(" CSV文件为空")
                return

            # 计数器用于控制休息间隔
            movie_count = 0

            for idx, row in enumerate(rows, start=1):
                # 从用户指定的位置开始
                if idx < start_index:
                    continue

                url = row.get('详情链接')
                if not url:
                    print(f" 第 {idx} 行没有包含‘详情链接’字段")
                    continue

                movie_id = get_movie_id(url)
                if not movie_id:
                    print(f" 第 {idx} 行无法从链接中提取电影ID: {url}")
                    continue

                print(f"\n 正在采集第 {idx} 部电影: {url}")
                result = fetch_movie_info(movie_id, get_default_headers())

                if result is None:
                    print(f" 第 {idx} 部电影信息抓取失败，跳过...")
                    continue

                print(f" 第 {idx} 部电影信息已成功采集并保存")

                # 增加计数器
                movie_count += 1

                # 每爬取5部电影休息5秒
                if movie_count % 5 == 0:
                    print(f"\n 已爬取{movie_count}部电影，休息5秒...")
                    time.sleep(5)

    except FileNotFoundError:
        print(f" 找不到CSV文件: {csv_path}")
    except Exception as e:
        print(f" 读取CSV文件失败: {e}")


if __name__ == "__main__":
    # 用户输入CSV文件路径和起始位置
    csv_path = input("请输入CSV文件的路径（相对或绝对路径）: ")
    start_index = int(input("请输入从第几部电影开始爬取（从1开始计数）: "))

    scrape_all_movies_from_csv(csv_path, start_index)