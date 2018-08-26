# python_module

This is a python api to access the mednicklab database. It implements all get and post endpoints for the database server. Please see the [MednickDB Endpoint](https://app.swaggerhub.com/apis/mednickAPI/mednick-db_api/1.0.0) documentation for more information on the specific endpoints.

## Authors

* **Ben Yetton**
* **Juan Antonio**
* **Nelly Lyu** 

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [Licence](LICENSE.md) file for details

## About File Versions and states:
Files have can have multiple states (expired, active, deleted) and multiple versions are permitted. 
If a file with the same name and info as an existing file is uploaded, the previous version will be marked as inactive and the new version will take its place.

- A file is *"active"* when its the most recent version. If you are querying files via ```mednick_api.get_files()```, 
then you should only get the active version. An addtional ```fileversion``` parameter may be supplied to get older versions.

- A file is *"expired"* when it is due for deletion, but will be maintained for some pre defined time before automatically being deleted. The raw file still exists at this point. 
The file can still be retrived via ```mednick_api.get_deleted_files()```.  
 
- A file is *"deleted"* when its raw file has been destroyed.
