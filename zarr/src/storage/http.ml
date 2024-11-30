module type S = sig
  exception Not_implemented
  exception Request_failed of int * string

  include Storage.STORE

  val with_open :
    ?redirects:int ->
    ?tries:int ->
    ?timeout:int ->
    string ->
    (t -> 'a Deferred.t) ->
    'a Deferred.t
end

module type C = sig
  include module type of Ezcurl_core
  include Ezcurl_core.S
end

module Make (Deferred : Types.Deferred) (C : C with type 'a io = 'a Deferred.t) : S with module Deferred = Deferred = struct
  exception Not_implemented
  exception Request_failed of int * string

  open Deferred.Syntax

  let raise_error (code, s) =
    raise (Request_failed (Curl.int_of_curlCode code, s))

  let fold_result = Result.fold ~error:raise_error ~ok:Fun.id

  module IO = struct
    module Deferred = Deferred

    type t =
      {tries : int
      ;base_url : string
      ;client : C.t
      ;config : Ezcurl_core.Config.t}

    let get t key =
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let+ res = C.get ~tries ~client ~config ~url () in
      match fold_result res with
      | {code; body; _} when code = 200 -> body
      | {code; body; _} -> raise (Request_failed (code, body))

    let size t key =
      try
        let+ data = get t key in
        String.length data
      with
      | Request_failed (404, _) -> Deferred.return 0
    (*let size t key =  
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let type' = if String.ends_with ~suffix:".json" key then "json" else "octet-stream" in
      let headers = [("Content-Type", "application/" ^ type')] in
      let res = Ezcurl.http ~headers ~tries ~client ~config ~url ~meth:HEAD () in
      match fold_result res with
      | {code; _} when code = 404 -> 0
      | {headers; _} ->
        match List.assoc_opt "content-length" headers with
        | (Some "0" | None) ->
          begin try print_endline "empty content-length header"; String.length (get t key) with
          | Request_failed (404, _) -> 0 end
        | Some l -> int_of_string l *)

    let is_member t key =
      let+ s = size t key in
      if s = 0 then false else true

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
      | {code; _} when code = 200 || code = 201 -> ()
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

  let with_open ?(redirects=5) ?(tries=3) ?(timeout=5) url f =
    let config = Ezcurl_core.Config.(default |> max_redirects redirects |> follow_location true) in
    let perform client = f IO.{tries; client; config; base_url = url ^ "/"} in
    let set_opts client = Curl.set_connecttimeout client timeout in
    C.with_client ~set_opts perform

  include Storage.Make(IO)
end
