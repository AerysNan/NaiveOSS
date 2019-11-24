# NaiveOSS

## Design

NaiveOSS is a simple object storage system implemented in Golang. There are five main characters in NaiveOSS listed below.

### Client

Client sends OSS commands to proxy server.

### Proxy Server

Proxy server acts as a HTTP server which translate clients' HTTP request into gRPC request that can be handled by our internal structure. Proxy server maintains a stable connection with metadata server and a connection pool with multiple storage servers. Except for connections it holds proxy server is mainly a stateless component.

### Metadata Server

Metadata server holds objects' metadata as well as their physical locations on storage servers. It also acts as a global controller for data deduplication and garbage collection. Metadata server imitates the design of LevelDB, containing a read / write layer in memory and multiple read only layers on disks arranged as sorted string tables. In each layer there are several keys, and each key indicates the metadata of an object. When proxy server receives a put / get request, it will first consult metadata server whether the object exists and where its physical location is, and sends the message back to proxy server.

### Storage Server

Storage server stores actual object contents. Usually there are multiple storage servers and metadata server confirms their existence by repeatedly receiving heartbeat request from them. The minimal unit of storage management is called volume. Each storage server has only one active volume and multiple closed volumes. After proxy server receives the data location, it directly puts to / gets from the storage server whose address is told by metadata server.

### Authentication Server

Authentication server manages users' authentication to access and manipulate data. When a user logins in from a client, proxy server will send the login request to authentication server to check if it has correct username and password. If so, authentication server will generate a JWT token which contains the username and user permission level. After logging in, each OSS request's token will first be redirected to authentication to get permission. If failed the request will be rejected and failed.

## Usage

### Proxy Server

```
usage: proxy [<flags>]
Flags:
  --help                      Show context-sensitive help (also try --help-long and --help-man).
  --address="127.0.0.1:8082"  listen address of proxy server
  --meta="127.0.0.1:8081"     listen address of meta server
  --auth="127.0.0.1:8083"     listen address of auth server
  --debug                     use debug level of logging
```

### Metadata Server

```
usage: metadata [<flags>]
Flags:
  --help                      Show context-sensitive help (also try --help-long and --help-man).
  --address="127.0.0.1:8081"  listen address of metadata server
  --root="../data"            metadata file path
  --config="../config/metadata.json"
                              config file full name
  --debug                     use debug level of logging
```

The config file should be a JSON file which contains the following fields:
```json
{
  "DumpFileName": "******",     // dump file name of metadata server
  "LayerKeyThreshold": ******,  // maximum number of keys before rotating a RW layer to RO
  "TrashRateThreshold": ******, // maximum trash rate before compressin a layer
  "DumpTimeout": ******,        // dump loop time in nanosecond
  "HeartbeatTimeout": ******,   // heartbeat timeout from storage server in nanosecond
  "CompactTimeout": ******,     // compact loop time in nanosecond
  "GCTimeout": ******           // garbage collection loop time in nanosecond
}
```

### Storage Server

```
usage: storage [<flags>]
Flags:
  --help                       Show context-sensitive help (also try --help-long and --help-man).
  --address="127.0.0.1:8080"   listen address of storage server
  --metadata="127.0.0.1:8081"  listen address of metadata server
  --root="../data"             metadata file path
  --config="../config/storage.json"
                               config file full name
  --debug                      use debug level of logging
```

The config file should be a JSON file which contains the following fields:
```json
{
  "DumpFileName": "******",  // dump file name of storage server
  "VolumeMaxSize": ******,   // maximum size before closing a writable volume
  "DumpTimeout": ******,     // dump loop time in nanosecond
  "HeartbeatTimeout": ****** // heartbeat loop time in nanosecond
}
```

### Authentication Server

```
usage: auth [<flags>]
Flags:
  --help                      Show context-sensitive help (also try --help-long and --help-man).
  --address="127.0.0.1:8083"  listen address of auth server
  --root="../data"            database file path
  --config="../config/auth.json"
                              config file full name
  --debug                     use debug level of logging
```

```json
{
  "AuthDBFileName": "******",     // authentication database name
  "SuperUserName": "******",      // intitial super user name
  "SuperUserPassword": "******",  // initial super user password
  "JWTSecretKey": "******"        // secret key for JWT encoding
}
```

## Function

### User Actions

#### User Login

`client login --user=USER --pass=PASS`

User login command gets a JWT token which represents your identity and stores it locally. We assume you neither delete this token nor leak it to other users. Initially you can only login with username and password specified in `auth.json`. Other users can be created later by super user (see below).

### User Logout

`client logout`

Simply delete the local JWT token.

### Create Bucket

`client create-bucket=BUCKET`

Create an OSS bucket. Bucket can be viewed as namespace. A bucket name and a key together forms a globally unique identifier for each object.

### Delete Bucket

`client delete-bucket=BUCKET`

Delete an OSS bucket.

### Get object

`client get --bucket=BUCKET --key=KEY`

Get an object with its bucket name and its key.

### Put Object

`client put --bucket=BUCKET --key=KEY --file=FILE`

Put an object to a given bucket with a given key. The file parameter specifies the file path of the object to be put.

### Delete Object

`client delete --bucket=BUCKET --key=KEY`

Delete an object in a given bucket with a given key.

### Get Object Head

`client head --bucket=BUCKET --ke=KEY`

Similar to get object command, but only retrieve the metadata of the object.

### Create User

`client register --user=USER --pass=PASS --role=ROLE`

Create a new user with given username and password. The role paarmeter specifies whether it will be created as super user or not. Zero means non-super user and one means super user.

### Give User Permission

`client grant --user=USER --bucket=BUCKET --level=LEVEL`

Give a user with given username a certain level of permission on the given bucket. There are four permission levels, i.e. `NONE, READ, WRITE, OWNER`, correspondingly zero, one, two and three. The meaning of permission levels will be discussed below.

### Internal Features

#### Garbage Collection

As mentioned above, metadata server maintains a multiple layer structure in each bucket. Since our index struct is append-only, we have to search from the newest layer to the oldest until found when handling a OSS get request. Although we can perform binary search in each layer, there will be a heavy disk I / O overhead since read onliy layers are stored on disks. To alleviate this performance degradation, we frequently compact several read only layers into one. When performing compaction, we scan all the indexes in all the involved layers to find which keys are overwritten by newer keys. By doing this we can calculate a trash rate for each volume, indicating the portion of useless space in each volume. If it excceeds a certain threshold, we perform compression, migrating all available objects to a new volume.

Besides volume compaction and volume compression, we also maintains a reference map for each volume, indicating how many layers a volume is referencedd by. When a read / write layer rotates into a read only layer. All its corresponding volumes' reference counts are increased by one. When serveral layers are compacted into one, all the old layers' volumes' reference counts are decreased by one. When a volume's reference count reaches zero, metadata server will send a delete volume request to storage server to free useless space.

#### Data Deduplication

We save a `SHA256` value for each object put. After getting a `put object` request, we first calculate its `SHA256` value and check if it already exists. If so we simply create a metadata entry pointing to the already existing object content address in the storage server without sending actual data to it.

#### Data Redundancy

Storage server applies a virtual layer to offer volume level service. However data is actually stored as shards using Reed-Solomon codes. A volume can be easily recontructed if no more than 1/3 of all shards are broken.

#### Authentication

Authserver use sqlite to store two tables. The user table stores username, password and whether a user is super user or  not. The privilege table stores users' permission level on each bucket. If a user is super user, then it can always pass the authentication check, or it must have a certain permission level to perform any OSS action. The required permission levels are listed as below:

| Action | Minimum Permission Level | Comment |
| :----: | :----: | :---: |
| `create-bucket` | `NONE` | User automatically gets owner level of permission on the bucket. |
| `delete-bucket` | `OWNER` | |
| `put` | `WRITE` | |
| `get` | `READ` | |
| `delete` | `WRITE` | |
| `head` | `READ` | |
| `register` | / | Only super user can create user. |
| `grant` | / | The granter's permission level should be no higher than the granted permission level. |

#### Resume from Breakpoint

The `get object` action can be easy to resume from breakpoint if we add the offset field to each get request. To handle resuming in `put object` action, we split put into three simpler steps: `connect`, `transport` and `disconnect`. In `transport` step object body is sent in data chunks. Chunks are piled in temporary folder in the storage server until the client sends `disconnect` to confirm the object has been fully sent. Then chunks are assembled into a volume to be stored in the storage server.