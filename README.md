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
                other infomation:
                    credentials: https://drive.google.com/file/d/1j3bS0tt10QBX9eAAxdQZFdOjOq9IlNbb/view
                    [minio]: https://drive.google.com/file/d/134Ii0cqNDnDhjx2T1gANV_TAgeXBqTqx/view?usp=sharing
    Quy tắc tạo file excel
        Bắt buộc phải có 2 sheet và tên như sau
            Sheet [data]: chứa dữ liệu sẽ được load lên table DWH
            Sheet [metadata]: Dùng để mapping cột trong Sheet [data] lên table DWH
            
        metadata: 
            cellB1:tên table trong db
            cellB2:tên cột chứa kiểu dữ liệu timestamp để tham chiếu trong fact
            cellA6:col_name_excel:tên column của sheet data trong file excel
            cellB6:col_name_des:tên column trong db tương ứng
            cellC6:type_of_col_des: kiểu dữ liệu của cột trong db(postgress-sql)

            
            


            