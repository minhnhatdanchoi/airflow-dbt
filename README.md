# Setup guide
    Setup airflow server:
    Create .env file with content :AIRFLOW_UID=50000
        run docker:
            docker compose init
            docker compose up --build
    Config airflow:
        import variable file: variables.json
            url: https://drive.google.com/file/d/16o5RqEz4asFTyKJlc3zFlwPz6FcU6ltX/view?usp=sharing
        create connection:
            - postgres:
                connection id: dwh_connection
                other infomation:
                    [DB dev dwh]: https://docs.google.com/spreadsheets/d/15kSTaDU_USQQOCsTKjdF2N7_VDTllm_kmazj171SuI4/edit?gid=22832642#gid=22832642
            - minio:
                connection id: minio_s3_conn
                connection type: Amazon Web Services
                AWS Access Key ID: something like this: 79KRH25otdwym5l8g1Ef
                AWS Secret Access Key: something like this: 9kCi9R4uJiznNuyHyOnKWDE9ZzR0o91wHy9YOvGY
                add this to extra:
                {
                "endpoint_url": "10.1.12.78:9000" (replace with your endpoint)
                }
    Quy tắc tạo file excel: (Push this file excel to minio then airflow will detect it and create dag)
        Bắt buộc phải có 2 sheet và tên như sau
            Sheet [data]: chứa dữ liệu sẽ được load lên table DWH
            Sheet [metadata]: Dùng để mapping cột trong Sheet [data] lên table DWH
            
        metadata: 
            cellB1:tên table trong db
            cellB2:tên cột chứa kiểu dữ liệu timestamp để tham chiếu trong fact
            cellA6:col_name_excel:tên column của sheet data trong file excel
            cellB6:col_name_des:tên column trong db tương ứng
            cellC6:type_of_col_des: kiểu dữ liệu của cột trong db(postgress-sql)

            
#Function detail:
Full pipeline:
	ETL task : Scan all file excel in MinIO (Amazone web service like S3) each file have data and metadata sheet rule like above
			   Each excel file will be a dag in airflow ( set schedule to run monthly - easy to know which one is error), after run it will push data to bronze schema ( posgresql )
	DBT task:  Transform bronze to silver (remove null,....)
			   Transform to gold (dataset)
		   
		   


            