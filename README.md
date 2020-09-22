# Finding-Near-Duplicate-Lyrics
The aim of this project is to find near duplicate lyrics to determine similar artists. Python 3 is used for programming this project.
## Techniques
There are 2 different implementation. First one is using k-Shingling or word-based Shingling (depending on choice), MinHashing and Locality Sensitive Hashing (LSH). Other one is using a MapReduce function which is executed using a virtual machine in Google Cloud Platform.
## Dataset
1st implementation uses a CSV file and 2nd implementation uses a text file. Both of them must have 3 columns which are artist, song name, lyrics. This data can be gathered using crawler or download from kaggle. Crawler is provided in this repository.
## Results and Comparison
1st approach is much slower than MapReduce implementation. The reason behind this result is that MapReduce uses parallelization, and depending on the number of workers the runtime shortens. 1st approach took around 8 to 9 hours and MapReduce took at most 45 minutes.
## Contributors
* Emre Gürçay
* Hüseyin Emre Başar
* Erdem Adaçal
* Çağatay Küpeli
