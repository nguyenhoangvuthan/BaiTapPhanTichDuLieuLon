"""Tải đầu vào đúng phiên bản. Chạy từ bất kỳ thư mục nào; không ghi đè tệp có sẵn."""
from pathlib import Path
from urllib.request import urlretrieve

ROOT=Path(__file__).resolve().parents[1]
SOURCES={
    'nyc_data/yellow_tripdata_2023-01.parquet':'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet',
    'nyc_data/yellow_tripdata_2023-02.parquet':'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet',
    'nyc_data/taxi_zone_lookup.csv':'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
    'aml_data/HI-Small_Trans.csv':'https://www.kaggle.com/api/v1/datasets/download/ealtman2019/ibm-transactions-for-anti-money-laundering-aml/HI-Small_Trans.csv?datasetVersionNumber=8',
}

def main():
    for relative,url in SOURCES.items():
        path=ROOT/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        if path.exists():
            print('Đã có:',relative,path.stat().st_size,'bytes')
            continue
        temporary=path.with_suffix(path.suffix+'.part')
        print('Đang tải:',relative,flush=True)
        urlretrieve(url,temporary)
        temporary.replace(path)
        print('Hoàn tất:',path.stat().st_size,'bytes')

if __name__=='__main__':main()
