open Hobbes.Helpers.Environment
open Hobbes.Web
open Hobbes.Messaging
open Hobbes.Messaging.Broker
open Hobbes.FSharp.Compile
open Hobbes.Web.RawdataTypes
open Hobbes.Workers.ODataProvider
open Thoth.Json.Net

let private handleMessage message =
    match message with
    Empty -> Success
    | Sync(name,configDoc) -> 
        Log.debugf "Received message. %s" configDoc
        try
            let configuration = 
                configDoc |> Config.Parse
            let meta = 
                match configuration.Source.Meta.JsonValue with
                FSharp.Data.JsonValue.Record properties -> properties
                | v -> failwithf "Expected an object got %A" v
            let source = configuration.Source
            let data = 
                {
                    Filter = source.Filter
                    Select = source.Select
                    Expand = source.Expand
                    User   = source.User
                    Pwd    = source.Pwd
                    Url    = source.Url.Value
                } |> Hobbes.Workers.ODataProvider.read
            let transformation = 
                match (configuration.Transformation |> Hobbes.FSharp.Compile.compile).Head.Blocks
                    |> Seq.filter(function
                                    Transformation _ -> true
                                    | _ -> false
                    ) |> Seq.exactlyOne with
                Transformation t -> t
                | _ -> failwith "should have been removed"
            let dataAsJson = 
                data 
                |> Hobbes.FSharp.DataStructures.DataMatrix.fromTable
                |> transformation
                |> Hobbes.FSharp.DataStructures.DataMatrix.toJson
            printfn "Transformation result: %s" (dataAsJson |> Encode.toString 0)
            let result = 
                let rec encode = 
                    function
                        FSharp.Data.JsonValue.String s -> Encode.string s
                        | FSharp.Data.JsonValue.Number d -> Encode.decimal d
                        | FSharp.Data.JsonValue.Float f -> Encode.float f
                        | FSharp.Data.JsonValue.Boolean b ->  Encode.bool b
                        | FSharp.Data.JsonValue.Record properties ->
                            properties
                            |> Array.map(fun (name,v) ->
                                name, v |> encode
                            ) |> List.ofArray
                            |> Encode.object
                        | FSharp.Data.JsonValue.Array elements ->
                             elements
                             |> Array.map encode
                             |> Encode.array
                        | FSharp.Data.JsonValue.Null -> Encode.nil
                                   
                              
                Encode.object [
                    "data",dataAsJson
                    "name", Encode.string name
                    "meta", 
                       meta
                       |> Array.map(fun (name,value) ->
                           name, value |> encode
                       ) |> List.ofArray
                       |> Encode.object
                ] |> Encode.toString 0
            
            match Http.post (Http.UniformData Http.Update) result with
            Http.Success _ -> 
               Log.logf "Data uploaded to cache"
               Success
            | Http.Error(status,msg) -> 
                sprintf "Upload of %s to uniform data failed. %d %s" result status msg
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
        Broker.OData handleMessage
    } |> Async.RunSynchronously
    0