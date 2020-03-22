from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatingsCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort)
        ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sort(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    MovieRatingsCount.run()