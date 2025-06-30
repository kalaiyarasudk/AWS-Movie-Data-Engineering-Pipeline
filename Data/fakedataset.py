import csv
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Genres and Certificates to pick from
genres = [
    "Action", "Adventure", "Animation", "Comedy", "Crime", "Drama",
    "Fantasy", "Horror", "Mystery", "Romance", "Sci-Fi", "Thriller"
]

certificates = ["G", "PG", "PG-13", "R", "NC-17", "U", "A", "UA"]

# Create 2000 rows of movie data
num_rows = 2000
output_file = "synthetic_movies_dataset.csv"

with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "Poster_Link", "Series_Title", "Released_Year", "Certificate",
        "Runtime", "Genre", "IMDB_Rating", "Overview", "Meta_score",
        "Director", "Star1", "Star2", "Star3", "Star4", "No_of_Votes", "Gross"
    ])

    for _ in range(num_rows):
        title = fake.catch_phrase()
        year = random.randint(1950, 2024)
        certificate = random.choice(certificates)
        runtime = f"{random.randint(80, 180)} min"
        genre = ", ".join(random.sample(genres, k=random.randint(1, 3)))
        imdb_rating = round(random.uniform(4.0, 9.5), 1)
        meta_score = random.choice([random.randint(30, 100), None])
        votes = random.randint(10_000, 1_000_000)
        gross = random.choice([round(random.uniform(1305, 936662225), 2), None])
        overview = fake.sentence(nb_words=20)
        director = fake.name()
        stars = [fake.name() for _ in range(4)]
        poster_link = f"https://dummyimage.com/600x800/000/fff&text={title.replace(' ', '+')}"

        writer.writerow([
            poster_link, title, year, certificate, runtime, genre,
            imdb_rating, overview, meta_score, director,
            stars[0], stars[1], stars[2], stars[3], votes, gross
        ])

print(f"✅ Generated {num_rows} rows in '{output_file}'")
