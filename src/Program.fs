open Hobbes.Helpers.Environment
open Readers.AzureDevOps.Data
open Readers.AzureDevOps
open Hobbes.Web
open Hobbes.Messaging
open Hobbes.Messaging.Broker

let synchronize (source : AzureDevOpsSource.Root) token =
    try
        Reader.sync token source
        |> Some
    with e ->
        Log.excf e "Sync failed due to exception"
        None
    |> Option.bind(fun (statusCode,body) ->
        if statusCode >= 200 && statusCode <= 300 then 
            Log.debugf "Sync finised with statusCode %d" statusCode
            match Reader.read source with
            _,None -> failwith "Could not read data from raw"
            | key,Some d -> Some (key, d)
        else
            Log.errorf  "Syncronization failed. %d Message: %s" statusCode body
            None                 
    )

let handleMessage message =
    match message with
    Empty -> Success
    | Sync sourceDoc -> 
        Log.debugf "Received message. %s" sourceDoc
        try
            let source = sourceDoc |> AzureDevOpsSource.Parse
            let getTokenName (s : string) = 
                s.Replace("-","_").ToUpper()
                |> sprintf "AZURE_TOKEN_%s"

            let token = 
                if System.String.IsNullOrWhiteSpace source.Server then 
                    if source.Account.ToString() = "kmddk" then
                        env "AZURE_TOKEN_KMDDK" null
                    else
                        env "AZURE_TOKEN_TIME_PAYROLL_KMDDK" null
                else 
                    let tokenName = 
                        source.Server.ToString().Split("/").[3]
                        |>getTokenName 
                    env tokenName null

            match synchronize source token with
            None -> 
                sprintf "Conldn't syncronize. %s %s" sourceDoc token
                |> Failure 
            | Some (key,data) -> 
                assert(System.String.IsNullOrWhiteSpace key |> not)
                assert(data.ColumnNames.Length > 0)
                assert(data.Values.Length = 0 || data.ColumnNames.Length = data.Values.[0].Length)
                
                let data = Cache.createCacheRecord key [] data
                match Http.post (Http.UniformData Http.Update) (data.ToString()) with
                Http.Success _ -> 
                   Log.logf "Data uploaded to cache"
                   Success
                | Http.Error(status,msg) -> 
                    sprintf "Upload of %s to uniform data failed. %d %s" (data.ToString()) status msg
                    |> Failure
        with e ->
            Log.excf e "Failed to process message"
            Excep e
            
[<EntryPoint>]
let main _ =
    Database.awaitDbServer()
    Database.initDatabases ["azure_devops_rawdata"]
    async{    
        do! awaitQueue()
        Broker.AzureDevOps handleMessage
    } |> Async.RunSynchronously
    0