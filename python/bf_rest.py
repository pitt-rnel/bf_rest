"""

"""

# import libraries
import requests
import os

# dictionary with all the requests that we use
class bf_rest:

    def __init__(self, api_key,api_secret):
        """
        When instantiated, this class saves the api key and secret,
        which can be obtained from user account settings
        in blacfynn web portal

        :param api_key: blackfynn api key
        :param api_secret: blackfynn api secret
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.sessionToken = ""
        self.organization = ''
        self.urls = {
            'init_session'    : 'https://api.blackfynn.io/account/api/session',
            'get_datasets'    : 'https://api.blackfynn.io/datasets/',
            'create_package'  : 'https://api.blackfynn.io/packages',
            'get_packages'    : 'https://api.blackfynn.io/datasets/<DID>/packages',
            'get_file'        : 'https://api.blackfynn.io/packages/<PID>/files/<FID>',
            'upload_preview'  : 'https://api.blackfynn.io/upload/preview/organizations/<OID>',
            'upload_chunk'    : 'https://api.blackfynn.io/upload/chunk/organizations/<OID>/id/<IID>',
            'upload_complete' : 'https://api.blackfynn.io/upload/complete/organizations/<OID>/id/<IID>',
            'upload_status'   : 'https://api.blackfynn.io/upload/status/organizations/<OID>/id/<IID>',
        }
        self.pageSize = 1000
        self.chunkSize = 5000000


    def initSession(self):
        """
        initiate the session with blackfynn web api using api key and secret
        provided when the class was instantiated

        :return: requestsResponse object
        """
        # curl command to create a session
        # curl 
        #   -X POST 
        #   https://api.blackfynn.io/account/api/session 
        #   -H "Content-Type: application/json" 
        #   -d "{
        #      \"tokenId\": \"d279a0c8-c0a9-4755-bdba-01103ba2c4e7\", 
        #      \"secret\": \"3e929a00-d9cf-4f42-aba0-ed4032648399\"}
        #

        # request new session
        r = requests.post(
            self.urls['init_session'],
            json = {
                'tokenId' : self.api_key,
                'secret' : self.api_secret,
            }
        )
        # save session token to be reused for all subsequent requests
        self.sessionToken = r.json()['session_token']
        self.organization = r.json()['organization']
        return r


    def getDatasets(self):
        """
        Returns all the info regarding all the datasets that the user has access to

        :return: dictionary containing the info for all the datasets
        """

        # place the correct request
        response = requests.get(
            self.urls['get_datasets'],
            params={
                'api_key' : self.sessionToken
            }
        )
        # returns the json format of the answer
        return response.json()


    def _provide_visual(self,visual=False):
        """
        Print "." for each time it is called.
        This function is meant to be used when there are long standing loops
        and it is useful to have this kind of feedback.

        :param visual: provide visual feedback when running long loops. Default: False
        :return: None
        """
        if visual:
            print(".",end="")
        #end if
    #end _provide_visual


    def createCollection(self,name,did,cid=None):
        """
        Create a new collection in the dataset specified at root level or in the collection specified as parent

        :param name:
        :param did: dataset blackfynn id
        :param cid: parent collection blackfynn id. If left empty, it will create the collection ar the root of the dataset
        :return: dictionary returned by the command
        """

        # POST
        # https://api.blackfynn.io/packages
        #  ?
        #  api_key=3bda6f7c-2265-4445-8f7c-d27d8a180f51
        #
        # Payload
        # {
        #  "name":"test2",
        #  "dataset":"N:dataset:ca906e73-9671-45b7-a25c-9df865777a60",
        #  "packageType":"Collection"
        # }
        #
        # https://api.blackfynn.io/packages
        #  ?
        #  api_key=3bda6f7c-2265-4445-8f7c-d27d8a180f51
        #
        # Payload
        # {
        #  "name":"test1_1",
        #  "parent":"N:collection:045bb60b-540c-42d7-9090-855d5aed87d9",
        #  "dataset":"N:dataset:ca906e73-9671-45b7-a25c-9df865777a60"
        # ,"packageType":"Collection"
        # }

        # prepares payload
        payload = {
            "name"        : name,
            "dataset"     : did,
            "packageType" : "collection"
        }

        if cid is not None:
            payload['parent'] = cid
        #end if

        # url
        url = self.urls['create_package']
        # post the request
        response = requests.post(
            url,
            params = {
                'api_key' : self.sessionToken
            },
            json = payload
        )

        # return json dictionary if successful, plain response if not
        return response.json() if response.status_code == 201 else response.content
    # end createCollection


    def getPackages(self,did,files=False,visual=False):
        """
        Retrieve all the packages in this dataset and returns the dictionary

        :param did: blackfynn dataset id
        :param files: retrieve source files too. Default: False
        :param visual: provide visual feedback for each request placed. Default: False
        :return: dictionary containing all the packages contained in this dataset
        """

        # define get url
        url = self.urls['get_packages'].replace('<DID>',did)
        # place get request
        r = requests.get(
            url,
            params = {
                'pageSize' : self.pageSize,
                'includeSourceFiles' : files,
                'api_key' : self.sessionToken
            }
        )
        # extract response in json format
        response = r.json();
        # extract data from response
        data = response['packages']
        self._provide_visual(visual)
        # we get only the first n packages with the first call
        # now we keep looping to get all the others
        while 'cursor' in response.keys():
            # place next request for next batch of packages
            r = requests.get(
                url,
                params = {
                    'pageSize' : self.pageSize,
                    'includeSourceFiles' : files,
                    'api_key' : self.sessionToken,
                    'cursor' : response['cursor']
                }
            )
            # extract response
            response = r.json();
            # append data
            data += response['packages']
            self._provide_visual(visual)
        # end while

        # return all the data retrieved
        return data
    #end getPackages


    def getFileContent(self,pid,fid):
        """
        Retrieve the file content, given the package and file id

        :param pid: blackfynn package id
        :param fid: balckfynn file id
        :return:
        """

        # test retrieving a file
        url = self.urls['get_file'].replace('<PID>',pid).replace('<FID>',str(fid))
        # get url to the file
        cr = requests.get(
            url,
            params = {
                'api_key' : self.sessionToken,
            }
        )
        # get file content
        fc = requests.get(cr.json()['url'])
        # return content as it is
        return fc.content
    #end getFileContent

    def downloadFile(self,pid,fid,filename):
        """
        Download file and save it with the file name given

        :param pid: blackfynn package id
        :param fid: blackfynn file id
        :param filename: local file path
        :return: none
        """

        # open file in writing
        with open(filename,'wb') as fh:
            # retrieve file content
            # and save it in the file just opened
            fh.write(
                self.getFileContent(pid,fid)
            )
        # end with
    # end downloadFile

    def uploadFile(self,cid,path,filename,oid=None):
        """
        upload the local file to the blackfynn container with the specified name

        :param cid: blackfynn id of the container  where the file should be saved
        :param path: local path to the file being uploaded
        :param filename: file name on blackfynn
        :param oid: blackfynn id of the organization. If not passed, it will used the organization id saved when session was initialized

        :return: dictionary containing the info provided by blackfynn when the upload has been complete
        """

        # check oid
        if oid is None:
            oid = self.organization
        #end if

        # create an upload preview
        # POST
        # https://api.blackfynn.io/
        #   upload/
        #   preview/
        #   organizations/
        #   N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0
        #     ?
        #     append=false&
        #     dataset_id=422&
        #     destinationId=N:collection:045bb60b-540c-42d7-9090-855d5aed87d9
        #
        #  or
        # https://api.blackfynn.io/
        #   upload/
        #   preview/
        #   organizations/
        #   N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0
        #     ?
        #     append=false&
        #     dataset_id=422
        #
        #  payload
        # {
        #  "files" : [
        #   {
        #    "uploadId" : 0,
        #    "fileName" : "mdfObjTemplate.json",
        #    "size" : 606,
        #    "importId" : "038694bb0-fcac-11e9-b50c-6595234c9b76",
        #    "processing":true,
        #    "file" : {
        #     "uploadId" : 0
        #    }
        #   }
        #  ]
        # }
        #
        # Response
        # {
        #  "packages" : [
        #   {
        #    "files" : [
        #     {
        #      "uploadId":1,
        #      "fileName":"mdfObjTemplate.json",
        #      "size":606,
        #      "multipartUploadId":"iQ41EO3fi92hTv33ON57gJijn8C9OuXQ71SCyRsWuZ8TeMFGRNEQ5jP.lEwYiSbTpCkXub97l0DJM4Bkcf9u8XAqJqP35G4dRpdxje6gplDp0oT8rVnDpjS2F_UBdEciC7E3DqcuEMmOB4TGHmyAtZeywbvbtO.2sm8jZGxuyz4-",
        #      "chunkedUpload": {
        #       "chunkSize":5242880,
        #       "totalChunks":1
        #      }
        #     }
        #    ],
        #    "packageName":"mdfObjTemplate",
        #    "packageType":"Unsupported",
        #    "packageSubtype":"JSON",
        #    "fileType":"Json",
        #    "warnings":[],
        #    "groupSize":606,
        #    "hasWorkflow":false,
        #    "importId":"e3c103b2-f3bb-4a78-9906-b1cc04079e9c",
        #    "icon":"JSON",
        #    "parent":null,
        #    "ancestors":null,
        #    "previewPath":null
        #   }
        #  ]
        # }
        #
        url = self.urls['upload_preview'].replace('<OID>',oid)
        preview_response = requests.post(
            url,
            params={
                'append'         : False,
                'destinationId'  : datasetId,
                'organizationId' : self.organization
            },
            headers={
                'accept'         : 'application/json',
                'Content-Type'   : 'application/json',
                'Authorization'  : 'Bearer ' + self.sessionToken
            },
            json = {
                'files': [
                    {
                        'uploadId': 1,
                        'fileName': filename,
                        'size': os.path.getsize(path),
                        'processing': False,
                    }
                ]
            }
        )

        # check if request was successful
        # if it failed, return content
        if preview_response.status_code != 201:
            return preview_response.content
        #end if

        # extract some values that are useful
        preview = preview_response.json()
        multipartId = preview['packages']['files'][0]['multipartUploadId']
        chunkSize = preview['packages']['files'][0]['chunkedUpload']['chunkSize']
        totalChunks = preview['packages']['files'][0]['chunkedUpload']['totalChunks']
        importId = preview['packages']['importId']

        # loop through the content and send all the chunks
        # POST
        # https://api.blackfynn.io/
        #  upload/
        #  fineuploaderchunk/
        #  organizations/
        #  N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/
        #  id/
        #  24f2fe8f-d4ef-4e13-8cf5-5160cc9fe944
        #    ?
        #    multipartId=6EY6PzYu.FpOkfjMCputCENngzOfA0mKG8gKWOKf6ilCUW8nMdFGZMHXNcAdcDRWGLpNfzbB92eNlnlMjXqhSS4q7wNhiu6qFlrLpdhaTDRHJ88CHD_tFD1gp2QMT2MYn4lq7X2f6Itkvo.7cB9MNVsL4ooOso4AOR.Y_yZb1Zw-
        # Payload
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqpartindex"
        #
        # 0
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqpartbyteoffset"
        #
        # 0
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqchunksize"
        #
        # 606
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqtotalparts"
        #
        # 1
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqtotalfilesize"
        #
        # 606
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqfilename"
        #
        # mdfObjTemplate.json
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qquuid"
        #
        # 8d2fa858-c5f8-4845-9042-456d85d55a04
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd
        # Content-Disposition: form-data; name="qqfile"; filename="blob"
        # Content-Type: application/octet-stream
        #
        # {
        #   "mdf_def": {
        #     "mdf_type" : "<MDF_TYPE>",
        #     "mdf_uuid" : "<MDF_UUID>",
        #     "mdf_vuuid" : "<MDF_VUUID>",
        #     "mdf_created" : "<MDF_CREATED>",
        #     "mdf_modified" : "<MDF_MODIFIED>",
        #     "mdf_children" : {
        #       "mdf_fields" : [],
        #       "mdf_types" : []},
        #     "mdf_data" : {
        #       "mdf_fields" : []},
        #     "mdf_files" : {
        #       "mdf_base" : "",
        #       "mdf_data" : "",
        #       "mdf_metadata" : ""},
        #     "mdf_links" : {
        #       "mdf_directions" : [],
        #       "mdf_fields" : [],
        #       "mdf_types" : []},
        #     "mdf_metadata" : {},
        #     "mdf_parents": [
        #     ]
        #   },
        #   "mdf_metadata": {
        #   },
        #   "mdf_version" : 1
        # }
        #
        # ------WebKitFormBoundary0mJbHoPiBnGBKxkd--
        # Response
        # {"success":true,"error":null}

        # get url for chunked upload
        url = self.urls['upload_chunk'].replace('OID',self.organization).replace('IID',importId)
        # initialize the hashing function
        hasher = hashlib.sha256();
        # open file in binary reading
        fh = open(path,'rb')
        fh.seek(0)
        # upload as many chunks are needed
        for chunk in range(totalChunks):
            # read chunk
            content = fh.read(chunkSize)
            # upadte hasher
            hasher.update(content)

            # upload
            chunk_response = requests.post(
                url,
                params={
                    'filename'       : filename,
                    'multipartId'    : multipartId,
                    'chunkNumber'    : chunk,
                    'chunkSize'      : length(content),
                    'chunkChecksum'  : hasher.hexdigest()
                },
                headers={
                    'Authorization'  : 'Bearer ' + self.sessionToken
                },
                data = content
            )

            # check results

        #end for

        # complete upload
        # POST
        # https://api.blackfynn.io/upload/complete/organizations/N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0/id/24f2fe8f-d4ef-4e13-8cf5-5160cc9fe944?datasetId=N:dataset:ca906e73-9671-45b7-a25c-9df865777a60&destinationId=N:collection:045bb60b-540c-42d7-9090-855d5aed87d9
        #
        # Response
        # [
        #  {
        #   "manifest" : {
        #    "type":"upload",
        #    "importId":"e3c103b2-f3bb-4a78-9906-b1cc04079e9c",
        #    "organizationId":367,
        #    "content" : {
        #     "packageId":589936,
        #     "datasetId":422,
        #     "userId":55,
        #     "encryptionKey":"arn:aws:kms:us-east-1:960751427106:key/23d4383c-dea2-4c59-ac71-2331527917ea",
        #     "files" : [
        #      "s3://prod-uploads-blackfynn/55/e3c103b2-f3bb-4a78-9906-b1cc04079e9c/mdfObjTemplate.json"
        #     ],
        #     "size":606
        #    }
        #   },
        #   "package" : {
        #    "content" : {
        #     "id":"N:package:ae7f0109-91ba-49bf-9852-832b07c2ba00",
        #     "nodeId":"N:package:ae7f0109-91ba-49bf-9852-832b07c2ba00",
        #     "name":"mdfObjTemplate",
        #     "packageType":"Unsupported",
        #     "datasetId":"N:dataset:ca906e73-9671-45b7-a25c-9df865777a60",
        #     "datasetNodeId":"N:dataset:ca906e73-9671-45b7-a25c-9df865777a60",
        #     "ownerId":55,
        #     "state":"UNAVAILABLE",
        #     "createdAt":"2019-11-01T13:40:12.414861Z",
        #     "updatedAt":"2019-11-01T13:40:12.414861Z"
        #    },
        #    "properties" : [
        #     {
        #      "category":"Blackfynn",
        #      "properties" : [
        #       {
        #        "key":"subtype",
        #        "value":"JSON",
        #        "dataType":"string",
        #        "fixed":false,
        #        "hidden":true,
        #        "display":"JSON"
        #       },
        #       {
        #        "key":"icon",
        #        "value":"JSON",
        #        "dataType":"string",
        #        "fixed":false,
        #        "hidden":true,
        #        "display":"JSON"
        #       }
        #      ]
        #     }
        #    ],
        #    "children":[]
        #   }
        #  }
        # ]

        # return response

    #end uploadFile



