from spark_sight.execute import _main


if __name__ == '__main__':
    
    _main(
        path_spark_event_log=r"C:\Users\a.fomitchenko\Downloads\spark-application-1655200731376.inprogress",
        cpus=20,
    ).show()
    
