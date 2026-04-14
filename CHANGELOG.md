# Version History

## 0.3.7.dev3

* Developer release (not public to pip)
* Update depends nativelib==0.2.5.dev3
* Add depends urllib3>=2.6.3
* Add depends brotli>=1.2.0
* Add more errors
* Add urllib3.HTTPConnectionPool for stream write big data into Clickhouse servers
* Fix vork with older clickhouse versions
* Refactor rust code
* Refactor HTTPCursor
* Rename rust classes
* Revision pyo3 crates
* Improve pytests

## 0.3.7.dev2

* Developer release (not public to pip)
* Update depends base-dumper==0.2.0.dev6
* Update depends csvpack==0.1.0.dev6
* Update depends nativelib==0.2.5.dev2

## 0.3.7.dev1

* Developer release (not public to pip)
* Update depends base-dumper==0.2.0.dev5
* Update depends csvpack==0.1.0.dev5
* Update depends light-compressor==0.1.1.dev2
* Update depends nativelib==0.2.5.dev1
* Update pyo3 revision to 0.28.3
* Update README.md

## 0.3.7.dev0

* Developer release (not public to pip)
* Update depends base-dumper==0.2.0.dev4
* Update depends csvpack==0.1.0.dev6
* Update depends light-compressor==0.1.1.dev1
* Update depends nativelib==0.2.5.dev0
* Add pytests
* Add NativeStreamReader class for read native stream
* Change pyo3 revision to 0.28.2
* Delete Code: 92. DB::Exception: (EMPTY_DATA_PASSED)
* Delete optional parameter waiting_data to HTTPCursor.get_stream() method
* Decompose repository
* Refactor python code
* Refactor pyo3http rust code
* Remove AbstractCursor import
* Remove HTTPCursor.header X-ClickHouse-Compression key
* Remove Rust source code from wheel package
* Update README.md

## 0.3.6.dev3

* Developer release (not public to pip)
* Update depends base-dumper==0.1.0.dev5
* Add TEST mode actions
* Add NativeDumper.user_agent parameter
* Add NativeDumper.dump_format parameter
* Add DumpFormat import
* Remove NativeDumper.s3fs parameter
* Refactor NativeDumper.mode_action() method
* Refactor NativeDumper._write_between() method
* Improve make_columns() function
* Improve DEBUG mode initialivation message
* Disabled session updates where not required

## 0.3.6.dev2

* Developer release (not public to pip)
* Update depends base-dumper==0.1.0.dev5
* Update depends light-compressor==0.1.0.dev3
* Update depends nativelib==0.2.3.dev4
* Add timeout=None to HttpSession
* Add size=None to HttpResponse.read()
* Add imports CompressionLevel, DumperLogger, DumperMode, IsolationLevel and Timeout
* Add queries folder with query_info.sql
* Add query_id key to HTTPCursor.params
* Add Debug info
* Add optional parameter waiting_data to HTTPCursor.get_stream() method. Default is False
* Change NativeDumper.compression_level default value to CompressionLevel.ZSTD_DEFAULT
* Delete HTTPCursor.check_length parameter
* Delete deprecated code from HTTPCursor
* Fix string_error() function

## 0.3.6.dev1

* Developer release (not public to pip)
* Update depends base-dumper==0.1.0.dev2
* Update depends light-compressor==0.1.0.dev2
* Update depends nativelib==0.2.3.dev3
* Add compression_level attribute to HTTPCursor
* Add compression_level attribute to NativeDumper

## 0.3.6.dev0

* Developer release (not public to pip)
* Add depends base-dumper==0.0.0.5
* Add mode parameter for future updates
* Add worked timeout and isolation parameters
* CHConnector now based on DBConnector
* Change hidden methods to protected methods
* Errors now based on BaseDumperError
* Fix HTTPCursor.get_response() get error for not connected server
* Fix HttpResponse.read() param in pyo3http.pyi
* Fix make_columns() function for DateTime64 value without timezone
* Fix write_between() method without dumper_src parameter
* HTTPCursor now based on AbstractCursor
* Move default CHConnector params from defines.py into connector.py
* Refactor code
* Remove depends light-compressor, sqlparse
* Remove deprecated param DBMS_DEFAULT_TIMEOUT_SEC from defines.py
* Removed duplicate code if it already in the base-dumper package

## 0.3.5.3

* Downgrade depends sqlparse>=0.5.4
* Fix error SQLParseError: Maximum number of tokens exceeded (10000)

## 0.3.5.2

* Change http_user_agent HTTPCursor -> NativeDumper 

## 0.3.5.1

* Fix chunk_query function

## 0.3.5.0

* Update depends light-compressor==0.0.2.2
* Update depends nativelib==0.2.2.6
* Update depends sqlparse>=0.5.5
* Change url link
* Change project development status to 4 - Beta

## 0.3.4.9

* Change server version view

## 0.3.4.8

* Update depends nativelib==0.2.2.5
* Fix build from source on unix systems

## 0.3.4.7

* Fix read query with comments in last line
* Back wheels to 3.10-3.14
* Back pyo3 to 0.26.0
* Back pyo3http to latest version
* Update depends light-compressor==0.0.2.1
* Update depends nativelib==0.2.2.4
* Update setuptools, wheel and setuptools-rust to latest versions

## 0.3.4.6

* Back wheels to 3.10-3.12
* Back depends light-compressor==0.0.1.9
* Back depends nativelib==0.2.2.2

## 0.3.4.5

* Downgrade rust module to pyo3==0.20

## 0.3.4.4

* Fix compile wheels

## 0.3.4.3

* Update depends light-compressor==0.0.2.0
* Update depends nativelib==0.2.2.3
* Downgrade compile depends to cython==0.29.33
* Make wheels for python 3.10 and 3.11 only

## 0.3.4.2

* Update depends nativelib==0.2.2.2

## 0.3.4.1

* Fix AttributeError: 'NoneType' object has no attribute 'strip'
* Disable Linux Aarch64

## 0.3.4.0

* Update depends sqlparse>=0.5.4

## 0.3.3.3

* Fix ; in query_src
* Change CHUNK_SIZE to 16KB

## 0.3.3.2

* Del chunks after write
* Add gc collect

## 0.3.3.1

* Improve chunk_query function

## 0.3.3.0

* Improve Multiquery decorator

## 0.3.2.3

* Update depends light-compressor==0.0.1.9
* Update depends nativelib==0.2.2.1
* Refactor variable info.precission -> info.precision

## 0.3.2.2

* Fix chunk_query for ignoring semicolons inside string literals

## 0.3.2.1

* Update depends nativelib==0.2.1.3

## 0.3.2.0

* Update depends light-compressor==0.0.1.8
* Update depends nativelib==0.2.1.2
* Fix multiquery wrapper
* Add transfer_diagram and DBMetadata to make log diagrams
* Add _dbmeta attribute
* Add log output diagram
* Add auto upload to pip

## 0.3.1.2

* Add NativeDumper.dbname attribute with constant string "clickhouse"
* Update setup.cfg

## 0.3.1.1

* Add wheels automake
* Update Cargo.toml depends
* Update depends nativelib==0.2.1.1
* Update pyo3http
* Change pyo3http.pyi
* Refactor HTTPCursor

## 0.3.1.0

* Add pyo3http.pyi files for pyo3 modules descriptions
* Add close() method to pyo3http.HttpSession
* Update MANIFEST.in
* Update depends setuptools>=80.9.0
* Update depends wheel>=0.45.1
* Update depends setuptools-rust>=1.12.0
* Update depends light-compressor==0.0.1.7
* Update depends nativelib==0.2.1.0

## 0.3.0.4

* Update depends nativelib==0.2.0.7
* Add internal methods __read_dump, __write_between and __to_reader to force kwargs creation

## 0.3.0.3

* Update requirements.txt depends nativelib==0.2.0.5
* Update requirements.txt depends light-compressor==0.0.1.6
* Update file_writer set chunk size to 1MB
* Add check error to execute function
* Add readed and sending size output into log
* Fix logger create folder in initialize
* Fix error code detector

## 0.3.0.2

* Change log message
* Improve refresh database after write

## 0.3.0.1

* Add attribute is_connected to HTTPCursor
* Add attribute server_version to HTTPCursor
* Add close files after read/write operations
* Add special error 92 (EMPTY_DATA_PASSED) when server sending empty data
* Improve login error
* Improve other errors
* Improve read method from another database
* Change log messages for read operations
* Update requirements.txt depends nativelib==0.2.0.4
* Update requirements.txt depends light-compressor==0.0.1.5

## 0.3.0.0

* Redistribute project directories
* Update requirements.txt
* Update README.md
* Change requests to rust pyo3 class
* Change methods & work strategy
* Readed dumps now not depends from compressed codecs
* Add support for write between with differents Databases (not ClickHouse only)
* Add MIT License

## 0.2.0.1

* Update depends in requirements.txt
* Change compressors to light-compressor
* Speed-up stream read

## 0.2.0.0

* Add nativelib to requirements.txt
* Add HTTPCursor.metadata(table) method for describe table columns
* Add NativeDumper.to_reader(query, table_name) method for return NativeReader object
* Add NativeDumper.from_rows(dtype_data, table_name) method for write table from python list
* Add NativeDumper.from_pandas(data_frame, table_name) method for write table from pandas.DataFrame
* Add NativeDumper.from_polars(dtype_data, table_name) method for write table from polars.DataFrame
* Update README.md

## 0.0.1.0

First version of the library native_dumper

* Read and write native format between clickhouse server and file
* Write native between clickhouse servers
