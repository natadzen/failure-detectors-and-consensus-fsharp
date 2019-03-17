namespace NetworkServer

open MBrace.FsPickler
open Newtonsoft.Json
open System.Text

module MessageSerialization =

    let serializeNewtonsoft message = async {
        let settings = JsonSerializerSettings(TypeNameHandling = TypeNameHandling.All, CheckAdditionalContent = true)
        return Encoding.ASCII.GetBytes(JsonConvert.SerializeObject(message, settings))
    }

    let deserializeNewtonsoft fromBytes = async {
        let settings = JsonSerializerSettings(TypeNameHandling = TypeNameHandling.All, CheckAdditionalContent = true)
        try
            return JsonConvert.DeserializeObject(Encoding.ASCII.GetString(fromBytes, 0, fromBytes.Length), settings)
        with
        | ex ->
            return failwith (sprintf "Exception deserializing object: %s." ex.Message)
    }

    // Alternative (not used in this project)
    let serializePickle message = async {
        let binarySerializer = FsPickler.CreateBinarySerializer()
        return binarySerializer.Pickle message
    }

    let deserializePickle<'a> fromBytes = async {
        let binarySerializer = FsPickler.CreateBinarySerializer()
        return binarySerializer.UnPickle<'a> fromBytes
    }