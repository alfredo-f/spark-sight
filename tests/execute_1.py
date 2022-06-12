from spark_sight.execute import _main


if __name__ == '__main__':
    
    _main(
        path_spark_event_log=r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\tests\spark_event_logs\spark-application-1652265340655 (medium 2 real world).inprogress",
        cpus=64,
    ).show()
    
