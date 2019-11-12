# Prepare sample data

## Prerequisite

* Python3 and libraries for development
  * `sudo yum install -y python3 python3-devel`
* Python3 packages
  * `pip3 install numpy pandas dask dask[dataframe] dask[distributed] toolz psutil`

## Installation

* Clone this repository
  * `git clone https://github.com/mnms/tutorials`
* Walk inside this `prep-sample-data` directory
  * `cd prep-sample-data/`

## Run script

* Run Python script to generate sample data
  * `python3 prep.py`
  * It may take 2~3 minutes for generate almost 51,800,000 rows with 24 files.

## Options

There're several options to generate sample data.

| Argument     | Usage                | Default               | Description                                      |
| ------------ | -------------------- | --------------------- | ------------------------------------------------ |
| --help       | --help               |                       | show help message                                |
| --start DTM  | --start 201912071230 | 201903011230          | set starting datetime with `yyyymmddHHMM` format |
| --count INT  | --count 3            | 24                    | set how many files to generate                   |
| --scale INT  | --scale 8            | 5                     | set how many rows to generate per file           |
| --worker INT | --worker 12          | 8                     | set how many processes to run parallelly         |
| --outdir DIR | --outdir /a/b/c/data | ./loadData            | set directory path to save generated files       |

## Example

* Command
  * The following command creates `12 workers` that generate `30 files`.
  * At this time, each file has a time value of 5 minutes intervals starting from `09: 00 on April 20, 1984`, and each file has more than `4M lines`.
  * Also, the generated files are stored in `~/sample-data`.

  ```sh
  python3 prep.py --start 198404200900 --count 30 --scale 10 --worker 12 --outdir ~/sample-data
  ```

* Result
  * The above command took 2 minutes and 35 seconds to run.
  * Let's look at the output files.

  ```sh
    # Count how man rows
    $ wc -l ~/sample-data/*.csv
      4039084 /home/ec2-user/sample-data/198404200900.csv
      4407299 /home/ec2-user/sample-data/198404200905.csv
      4523700 /home/ec2-user/sample-data/198404200910.csv
      4583207 /home/ec2-user/sample-data/198404200915.csv
      4486553 /home/ec2-user/sample-data/198404200920.csv
      4432518 /home/ec2-user/sample-data/198404200925.csv
      4184087 /home/ec2-user/sample-data/198404200930.csv
      4061729 /home/ec2-user/sample-data/198404200935.csv
      4508797 /home/ec2-user/sample-data/198404200940.csv
      4163869 /home/ec2-user/sample-data/198404200945.csv
      4478236 /home/ec2-user/sample-data/198404200950.csv
      4511293 /home/ec2-user/sample-data/198404200955.csv
      4478304 /home/ec2-user/sample-data/198404201000.csv
      4252531 /home/ec2-user/sample-data/198404201005.csv
      4603777 /home/ec2-user/sample-data/198404201010.csv
      4435624 /home/ec2-user/sample-data/198404201015.csv
      4199445 /home/ec2-user/sample-data/198404201020.csv
      4063136 /home/ec2-user/sample-data/198404201025.csv
      4096386 /home/ec2-user/sample-data/198404201030.csv
      4600275 /home/ec2-user/sample-data/198404201035.csv
      4067459 /home/ec2-user/sample-data/198404201040.csv
      4063233 /home/ec2-user/sample-data/198404201045.csv
      4557936 /home/ec2-user/sample-data/198404201050.csv
      4132154 /home/ec2-user/sample-data/198404201055.csv
      4000512 /home/ec2-user/sample-data/198404201100.csv
      4375469 /home/ec2-user/sample-data/198404201105.csv
      4391156 /home/ec2-user/sample-data/198404201110.csv
      4414736 /home/ec2-user/sample-data/198404201115.csv
      4489189 /home/ec2-user/sample-data/198404201120.csv
      4351782 /home/ec2-user/sample-data/198404201125.csv
    129953476 total

    # Take a look file
    $ head ~/sample-data/198404201015.csv
    event_time,name,sur_name,sex,city,lattitude,longitude,country
    198404201015,Oneta,STAUNER,girl,Karachi,24.8700,66.9900,Pakistan
    198404201015,Carry,GUZIK,girl,Bikin,46.8203,134.2649,Russia
    198404201015,Alana,ANDRYSIAK,girl,Buizhou,37.3704,118.0200,China
    198404201015,Vaughn,SMEAD,boy,Troy,42.7354,-73.6751,United States
    198404201015,Billy,BIGHORN,boy,Mount Pleasant,32.8533,-79.8269,United States
    198404201015,Stephan,LAUTHER,boy,La Puente,34.0323,-117.9533,United States
    198404201015,Darrin,DEBARD,boy,Savoy,40.0602,-88.2553,United States
    198404201015,Derick,HERMS,boy,Shaoxing,30.0004,120.5700,China
    198404201015,Ramsey,BUSKER,boy,Kendu Bay,-0.3596,34.6400,Kenya
  ```

## License

* Each file inside `./sampleData` directory retains its copyright.
  * `baby-names.csv`: N/A from [hadley/data-baby-names](https://github.com/hadley/data-baby-names/blob/master/baby-names.csv)
  * `surnames.csv`: [CC-BY-4.0](https://github.com/fivethirtyeight/data/blob/master/LICENSE) from [fivethirtyeight/data](https://github.com/fivethirtyeight/data/blob/master/most-common-name/surnames.csv)
  * `worldcities.csv`: [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/) from [simplemaps.com](https://simplemaps.com/data/world-cities)
