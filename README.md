
### mssql-sailsv1

Fork from the Official Microsoft SQL Server adapter, by [c*nect](http://www.cnectdata.com/), for [sails.js](http://sailsjs.org/). Tested on SQL Server 2017,
but should support any SQL Server 2005 and newer. 
This is an alpha version, there are several methods that needs to be implemented and i will keep implementing them over the time.

### 1. Install
```sh
$ npm install mssql-sailsv1 --save
```

### 2. Configure

#### `config/datastore.js`
```js
{
  adapter: 'mssql-sailsv1',
  url: 'mssql://usr:pwd@host:port/DB?encrypt=false'
}
```

```js
{
  adapter: 'mssql-sailsv1',
  user: 'cnect',
  password: 'pass',
  host: 'abc123.database.windows.net', // azure database
  database: 'mydb',
  options: {
    encrypt: true   // use this for Azure databases
  }
}
```
For more options to connect please check [mssql](https://github.com/tediousjs/node-mssql) documentation.

## License
MIT