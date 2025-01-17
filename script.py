import requests
import time
import csv
import random
from multiprocessing import Pool
import concurrent.futures
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}
MAX_THREADS = 10
MAX_PROCESSES = 4
CSV_FILE = 'movies.csv'

session = requests.Session()
session.headers.update(HEADERS)


def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0, 0.2))
        response = session.get(movie_link)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.find('h1').text.strip() if soup.find('h1') else None
        date = soup.find('a', href=lambda href: href and 'releaseinfo' in href)
        date = date.text.strip() if date else None
        rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        rating = rating_tag.text.strip() if rating_tag else None
        plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        plot_text = plot_tag.text.strip() if plot_tag else None

        if all([title, date, rating, plot_text]):
            return {"title": title, "date": date, "rating": rating, "plot": plot_text}

    except Exception as e:
        print(f"Erro ao processar {movie_link}: {e}")

    return None


def extract_movies():
    response = session.get('https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm')
    if response.status_code != 200:
        print("Erro ao acessar a página principal do IMDB.")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')

    if not movies_table:
        return []

    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table.find_all('li') if movie.find('a')]

    return movie_links


def use_threads(movie_links):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = list(executor.map(extract_movie_details, movie_links))

    save_to_csv([movie for movie in results if movie])


def use_processes(movie_links):
    with Pool(processes=MAX_PROCESSES) as pool:
        results = pool.map(extract_movie_details, movie_links)

    save_to_csv([movie for movie in results if movie])


def save_to_csv(movies):
    if not movies:
        return

    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
        fieldnames = ["title", "date", "rating", "plot"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        for movie in movies:
            writer.writerow(movie)


def compare_execution_times():
    movie_links = extract_movies()
    if not movie_links:
        print("Nenhum link de filme encontrado.")
        return

    print(f"Extraindo {len(movie_links)} filmes...")

    start_time = time.time()
    use_threads(movie_links)
    print(f"Tempo com Threads: {time.time() - start_time:.2f} segundos")

    start_time = time.time()
    use_processes(movie_links)
    print(f"Tempo com Processos: {time.time() - start_time:.2f} segundos")


if __name__ == '__main__':
    compare_execution_times()
