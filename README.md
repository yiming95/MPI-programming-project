# MPI-programming-project
MPI programming project in python to analyse twitter dataset.

### Introduction
This project is to implement a simple, parallelized application leveraging the University of Melbourne HPC facility SPARTAN.
The purpose of the application is to identify twitter usage from a large geocoded Twitter dataset.

The project uses python and MPI4py and it applies master-worker model. In this application, after the master node reads a batch of data, scatter will divide the data into pieces and sends them to the worker nodes. The worker nodes will count the number of tweets and hashtags. Then MPI_Gather takes the results from workers and gathers them to one result.

***

## Screenshots

<img width="1680" alt="Screen Shot 2019-04-03 at 10 02 34 pm" src="https://user-images.githubusercontent.com/40975373/56196441-2edca700-607a-11e9-9cda-25c672ac4560.png">

<img width="1017" alt="10000（2-8）" src="https://user-images.githubusercontent.com/40975373/56196518-52075680-607a-11e9-8b41-374ada5c728b.png">

***

## Authors

* **Yiming Zhang** 

## License

This project is licensed under the MIT License.
