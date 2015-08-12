import os, sys, inspect, csv, math
from StringIO import StringIO

### Note: Please set-up the environment variables before running the code:
### AWS_SECRET_ACCESS_KEY=...
### AWS_ACCESS_KEY_ID=...

### Current directory path.
curr_dir = os.path.split(inspect.getfile(inspect.currentframe()))[0]

### Setup the environment variables
spark_home_dir = os.path.realpath(os.path.abspath(os.path.join(curr_dir, "/root/spark/")))
python_dir = os.path.realpath(os.path.abspath(os.path.join(spark_home_dir, "./python")))
os.environ["SPARK_HOME"] = spark_home_dir
os.environ["PYTHONPATH"] = python_dir

### Setup pyspark directory path
pyspark_dir = os.path.realpath(os.path.abspath(os.path.join(spark_home_dir, "./python")))
sys.path.append(pyspark_dir)

### Setup the scode directory
scode_dir = os.path.realpath(os.path.abspath(os.path.join(curr_dir, "../IdeaNets/models/lstm/scode")))
sys.path.append(scode_dir)

### Setup the Synapsify directory
synapsify_dir = os.path.realpath(os.path.abspath(os.path.join(curr_dir, "../Synapsify")))
sys.path.append(synapsify_dir)

### from load_params import Load_LSTM_Params
from lstm_class import LSTM as lstm

### Import the pyspark
from pyspark import SparkConf, SparkContext

class SSpark():

    def __init__(self, params=None):
        default_params = {"root_dir":"/root/IdeaNets/IdeaNets/",
                          "S3_root":"s3n://synapsify-ruofan/Synapsify_data",
                          "app_name":"ruofan",
                          }
        if params!=None:
            for key,value in params.iteritems():
                try:
                    default_params[key] = value
                except:
                    print "Could not add: " + str(key) +" --> "+ str(value)
        self._params = default_params

    def spin_up(self):
        """
        If AWS clusters have not been initialized, start them up here.
        :return:
        """

        # Start AWS Master and slaves
        code here

        # ... wait for it
        # Now that they're spun up, designate them as master and slave
        self.assign_master_slave()



    def assign_master_slave(self,root_dir=None):
        ### Initialize the SparkConf and SparkContext

        ### Locations of Python files.
        if root_dir==None:
            root = self._root_dir
        else:
            # Do we want the user to be able to assign a root directory for the experiment after the object has been initialized.

        sheets_loc = os.path.realpath(os.path.abspath(os.path.join(root, '/Synapsify/loadCleanly/sheets.py')))
        lstm_class_loc = os.path.realpath(os.path.abspath(os.path.join(root, 'models/lstm/scode/lstm_class.py')))
        # AND SO ON AND SO FORTH... AND SO ON AND SO FORTH... AND SO ON AND SO FORTH...
        load_params_loc = '/root/IdeaNets/IdeaNets/models/lstm/scode/load_params.py'
        preprocess_loc = '/root/IdeaNets/IdeaNets/models/lstm/scode/synapsify_preprocess.py'

        ### Pass Python files to Spark.
        pyFiles = []
        pyFiles.append(sheets_loc)
        pyFiles.append(lstm_class_loc)
        pyFiles.append(load_params_loc)
        pyFiles.append(preprocess_loc)


        ### Initialize the spark configuration.
        master_url = ????? Ruofan/Elliott code
        conf = SparkConf().setAppName(self._app_name).setMaster(master_url)
        # conf = SparkConf().setAppName("ruofan").setMaster("spark://ec2-54-165-83-186.compute-1.amazonaws.com:7077")
        sc = SparkContext(conf = conf, pyFiles=pyFiles)

    def run_cluster(self):
        # ONCE THE MASTER AND SLAVES ARE ASSIGNED, RUN THE EXPERIMENT(S)
        ### Add non-python files passing to Spark.
        # THERE SHOULD BE A STANDARD STRUCTURE TO THE DIRECTORIES AFTER SOME INITIAL ROOT DIRECTORY SO THE CODE CAN BE STANDARDIZED
        sheets_loc = os.path.realpath(os.path.abspath(os.path.join(root, "/nonbreaking_prefix.en")))
        sc.addFile('/root/spark/bin/nonbreaking_prefix.en')
        sc.addFile('/root/IdeaNets/IdeaNets/models/lstm/scode/tokenizer.perl')
        sc.addFile('/root/IdeaNets/Synapsify/Synapsify/loadCleanly/stopwords.txt')
        sc.addFile('/root/IdeaNets/Synapsify/Synapsify/loadCleanly/prepositions.txt')


        datafile = sc.wholeTextFiles(self.S3_root, use_unicode=False) ### Read data directory from S3 storage.

        ### Sent the application in each of the slave node
        datafile.foreach(lambda (path, content): self.lstm_test(path, content))

    def collect_results(self):
        # IF THE NAME OF THIS FUNCTION ISN'T ENOUGH TO SAY WHAT IT DOES, I DON'T KNOW WHAT TO DO.
        # COLLECT IS FROM S3

    ### myfunc is to print the frist row for testing purpose.
    def myfunc(path, content):
        # CAN THIS BE A MEMBER FUNCTION, OR DOES IT NEED TO EXIST OUTSIDE THE CLASS
        ### Convert the string to the file object, and we need to import StringIO in the code.
        data = StringIO(content)

        cr = csv.reader(data)
        num_lines = sum(1 for line in cr)
        num_instances = num_lines - 1   ### The first line shouldn't be considered.
        train_size = int(math.ceil(num_lines / 2.0))
        test_size = int(num_instances - train_size)
        print "The total lines of ", path, " is: ", num_lines
        print "The training size is ", train_size
        print "The testing size is ", test_size

        for row in cr:
            #print "The first row of ", path, " is: ", row
            break

    def lstm_test(self, path, content):
      data = StringIO(content)

      ### Read data from S3.
      cr = csv.reader(data)
      num_lines = sum(1 for line in cr)
      num_instances = num_lines - 1   ### The first line shouldn't be considered.
      train_size = int(math.ceil(num_lines / 2.0))
      test_size = int(num_instances - train_size)

      ### Create an instance of lstm class
      params = {}
      params['raw_rows'] = content  ### Update the lstm
      params['train_size'] = train_size
      params['test_size'] = test_size
      params['class_type'] =  "Sentiment"

      run_lstm = lstm(params=params)
      run_lstm.build_model()
      run_lstm.train_model()
      run_lstm.test_model()

    # if __name__ == "__main__":
    #   main()


