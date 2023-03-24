from mrjob.job import MRJob
from mrjob.step import MRStep
import socket
import subprocess

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
        line = key+'/'+','.join(values)
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        yield line+'\t'+"ip :", ip_address

    def configure_args(self):
        super(Test, self).configure_args()
        self.add_file_arg('--input-file', help='path to input file')

    def mapper_init(self):
        input_file = self.options.input_file
        
        if input_file.startswith('hdfs://'):
            with subprocess.Popen(["hadoop", "fs", "-cat", input_file.split("hdfs://")[1]], stdout=subprocess.PIPE) as proc:
                self.file = proc.stdout
        else:
            self.file = open(input_file, 'r')

    def mapper_final(self):
        self.file.close()

if __name__ == '__main__':
    Test.run()