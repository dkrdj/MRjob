from mrjob.job import MRJob
from mrjob.step import MRStep
import socket

class Test(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        song = line.split('/')
        yield song[0], song[1]

    def reducer_count_ratings(self, key, values):
        line = key+'/'+values
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        yield line+'\t'+"ip :", ip_address

if __name__ == '__main__':
    Test.run()
