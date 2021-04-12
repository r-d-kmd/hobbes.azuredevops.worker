open Hobbes.Helpers.Environment
open Readers.AzureDevOps.Data
open Readers.AzureDevOps
open Hobbes.Web
open Hobbes.Messaging
open Hobbes.Messaging.Broker
open Hobbes.FSharp.Compile

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
    | Sync configDoc -> 
        Log.debugf "Received message. %s" configDoc
        try
            let config = 
                (configDoc |> Config.Parse).Transformation
                |> Hobbes.FSharp.Compile.compile
            let data = ODataProvider.read config.Source
            let CompiledBlock.Transformation(transformation) = 
                config.Blocks
                |> Seq.filter(function
                                Transformation _ -> true
                                | _ -> false
                ) |> Seq.exactlyOne
            let result = data |> transformation    
            
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