module type S = sig
  exception Not_implemented
  exception Request_failed of int * string
  include Storage.STORE

  type auth = {user : string; pwd : string}

  val with_open :
    ?basic_auth:auth ->
    ?redirects:int ->
    ?tries:int ->
    ?timeout:int ->
    string ->
    (t -> 'a Deferred.t) ->
    'a Deferred.t
  (** [with_open url f] connects to the Zarr store described by the url [url] 
      and applies function [f] to the store's open handle.

      {ul 
      {- [basic_auth] is the username and password to use for each request if
        required by the server.}
      {- [redirects] is the maximum number of redirects allowed per http request.
        Defaults to 5.}
      {- [tries] is the maximum number of times to retry a failed request.
        Defaults to 3.}
      {- [timeout] is the timeout for the connect phase. It sets the maximum
        time in seconds that you allow the connection phase to take. This
        timeout only limits the connection phase, it has no impact once the
        client has connected. The connection phase includes the name resolve
        (DNS) and all protocol handshakes and negotiations until there is an
        established connection with the remote side.
      } *)
end

module type C = sig
  include module type of Ezcurl_core
  include Ezcurl_core.S
end

module Make
  (Deferred : Types.Deferred)
  (C : C with type 'a io = 'a Deferred.t) : S with module Deferred = Deferred = struct
  exception Not_implemented
  exception Request_failed of int * string
  open Deferred.Syntax

  let raise_error (code, s) = raise (Request_failed (Curl.int_of_curlCode code, s))
  let fold_result = Result.fold ~error:raise_error ~ok:Fun.id

  module IO = struct
    module Deferred = Deferred
    open Deferred.Infix

    type t =
      {tries : int
      ;client : C.t
      ;base_url : string
      ;config : Ezcurl_core.Config.t}

    let get t key =
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let+ res = C.get ~tries ~client ~config ~url () in
      match fold_result res with
      | {code; body; _} when code = 200 -> body
      | {code; body; _} -> raise (Request_failed (code, body))

    let size t key = try get t key >>| String.length with
      | Request_failed (404, _) -> Deferred.return 0
    (*let size t key =  
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let type' = if String.ends_with ~suffix:".json" key then "json" else "octet-stream" in
      let headers = [("Content-Type", "application/" ^ type')] in
      let* res = C.http ~headers ~tries ~client ~config ~url ~meth:HEAD () in
      match res with
      | Error _ -> Deferred.return 0
      | Ok {code; _} when code = 404 -> Deferred.return 0
      | Ok {headers; code; _} when code = 200 ->
        begin match List.assoc_opt "content-length" headers with
        | Some "0" -> Deferred.return 0
        | Some l -> Deferred.return @@ int_of_string l
        | None ->
          begin try print_endline "empty content-length header";
          get t key >>| String.length with
          | Request_failed (404, _) -> Deferred.return 0 end
        end
      | Ok {code; body; _} -> raise (Request_failed (code, body)) *)

    let is_member t key =
      let+ s = size t key in
      if s > 0 then true else false

    let get_partial_values t key ranges =
      let tries = t.tries and client = t.client and config = t.config and url = t.base_url ^ key in
      let fetch range = C.get ~range ~tries ~client ~config ~url () in
      let end_index ofs l = Printf.sprintf "%d-%d" ofs (ofs + l - 1) in
      let read_range acc (ofs, len) =
        let none = Printf.sprintf "%d-" ofs in
        let range = Option.fold ~none ~some:(end_index ofs) len in
        let+ res = fetch range in
        let response = fold_result res in
        response.body :: acc
      in
      Deferred.fold_left read_range [] (List.rev ranges)

    let set t key data =
      let tries = t.tries and client = t.client and config = t.config
      and url = t.base_url ^ key and content = `String data in
      let type' = if String.ends_with ~suffix:".json" key then "json" else "octet-stream" in
      let headers =
        [("Content-Length", string_of_int (String.length data))
        ;("Content-Type", "application/" ^ type')] in
      let+ res = C.post ~params:[] ~headers ~tries ~client ~config ~url ~content () in
      match fold_result res with
      | {code; _} when code >= 200 && code < 300 -> ()
      | {code; body; _} -> raise (Request_failed (code, body))

    let set_partial_values t key ?(append=false) rsv =
      let* size = size t key in
      let* ov = match size with
        | 0 -> Deferred.return String.empty
        | _ -> get t key
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      set t key (List.fold_left f ov rsv)

    (* make reshaping arrays possible *)
    (*let erase t key =
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let+ res = C.http ~tries ~client ~config ~url ~meth:DELETE () in
      match fold_result res with
      | {code; _} when code = 200 || code = 404 -> ()
      | {code; body; _} -> raise (Request_failed (code, body)) *)

    let erase _ = raise Not_implemented
    let erase_prefix _ = raise Not_implemented
    let list _ = raise Not_implemented
    let list_dir _ = raise Not_implemented
    let rename _ = raise Not_implemented
  end

  type auth = {user : string; pwd : string}

  let default_cfg = Ezcurl_core.Config.(default |> follow_location true |> authmethod [CURLAUTH_ANY])

  let with_open ?(basic_auth={user=""; pwd =""}) ?(redirects=5) ?(tries=3) ?(timeout=5) url f =
    let set_opts client = Curl.set_connecttimeout client timeout in
    let perform client =
      let config = Ezcurl_core.Config.max_redirects redirects default_cfg
        |> Ezcurl_core.Config.username basic_auth.user
        |> Ezcurl_core.Config.password basic_auth.pwd
      in
      f IO.{tries; client; config; base_url = url ^ "/"}
    in
    C.with_client ~set_opts perform

  include Storage.Make(IO)
end
