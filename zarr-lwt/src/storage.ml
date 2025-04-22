module IO = struct
  type 'a t = 'a Lwt.t
  let return = Lwt.return_ok
  let error = Lwt.return_error
  let return_unit = Lwt_result.return ()
  let lift = Lwt_result.lift
  let bind = Lwt_result.bind
  let map = Lwt_result.map
  let rec fold_left f acc xs = match xs with
    | [] -> lift acc
    | x :: l -> bind (f acc x) (fun k -> fold_left f (Ok k) l)

  module Infix = struct
    let (>>=) = bind
    let (>>|) x f = map f x
  end

  module Syntax = struct
    let (let*) = Infix.(>>=)
    let (let+) = Infix.(>>|)
  end
end

module ZipStore = Zarr.Zip.Make(IO)
module MemoryStore = Zarr.Memory.Make(IO)

module FilesystemStore = struct
  module S = struct
    open IO.Infix
    open IO.Syntax

    type 'a io = 'a Lwt.t
    type t = {dirname : string; perm : int}
    type error = [ `Read of string | `Write of string ]

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      Lwt_result.ok (Lwt_unix.file_exists parent_dir) >>= function
      | true -> IO.return_unit
      | false ->
        let* () = create_parent_dir parent_dir perm in
        Lwt_result.ok (Lwt_unix.mkdir parent_dir perm)

    let size t key =
      let file_length path () = Lwt.map Int64.to_int (Lwt_io.file_length path)
      and path = key_to_fspath t key in
      Lwt_result.ok (Lwt.catch (file_length path) (fun _ -> Lwt.return 0))

    let get t key =
      let* x = size t key in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create x)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic -> Lwt_result.ok @@ Lwt_io.read ic)

    let get_partial_values t key ranges =
      let max_range ~tot acc (s, l) = match l with
        | None -> Int.max acc (tot - s) 
        | Some rs -> Int.max acc rs
      in
      let read_range ~tot ~ic (ofs, len) =
        Lwt.bind (Lwt_io.set_position ic @@ Int64.of_int ofs) @@ fun () ->
        match len with
        | None -> Lwt_io.read ~count:(tot - ofs) ic
        | Some count -> Lwt_io.read ~count ic
      in
      let* tot = size t key in
      let buf_size = List.fold_left (max_range ~tot) 0 ranges in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create buf_size)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic -> Lwt_result.ok @@ Lwt_list.map_s (read_range ~tot ~ic) ranges)

    let set t key value =
      let filename = key_to_fspath t key in
      let* () = create_parent_dir filename t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create (String.length value))
        ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT]
        ~perm:t.perm
        ~mode:Lwt_io.Output
        filename
        (fun oc -> Lwt_result.ok @@ Lwt_io.write oc value)

    let set_partial_values t key ?(append=false) rvs =
      let write_all rvs oc =
        let write ~oc (ofs, value) =
          Lwt.bind (Lwt_io.set_position oc @@ Int64.of_int ofs) (fun () -> Lwt_io.write oc value)
        in
        Lwt_list.iter_s (write ~oc) rvs
      in
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let flags = match append with
        | false -> Unix.[O_WRONLY; O_CREAT] 
        | true -> Unix.[O_APPEND; O_WRONLY; O_CREAT] 
      in
      let filepath = key_to_fspath t key in
      let* () = create_parent_dir filepath t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~perm:t.perm
        ~mode:Lwt_io.Output
        ~flags
        filepath
        (fun oc -> Lwt_result.ok @@ write_all rvs oc)

    let rec walk t acc dir =
      let accumulate ~t x a =
        if x = "." || x  = ".." then Lwt.return a else
        match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> Lwt.return (Result.map (List.cons (fspath_to_key t p)) a)
      in
      Lwt_stream.fold_s (accumulate ~t) (Lwt_unix.files_of_directory dir) acc

    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let predicate x = if x = "." || x = ".." then false else true in
      let dir = key_to_fspath t prefix in
      let relevant = Lwt_stream.filter predicate (Lwt_unix.files_of_directory dir) in
      Lwt_result.ok (Lwt.map (List.partition_map (choose ~t ~dir)) (Lwt_stream.to_list relevant))

    let list t = walk t (Ok []) (key_to_fspath t "")
    let list_prefix t prefix = walk t (Ok []) (key_to_fspath t prefix)
    let is_member t key = Lwt_result.ok (Lwt_unix.file_exists (key_to_fspath t key))
    let rm t key = Lwt_unix.unlink (key_to_fspath t key)
    let erase t key = Lwt_result.ok (rm t key)
    let erase_prefix t pre = Lwt_result.bind_lwt (list_prefix t pre) (Lwt_list.iter_s (rm t))
    let rename t k k' = Lwt_result.ok (Lwt_unix.rename (key_to_fspath t k) (key_to_fspath t k'))
  end

  let create ?(perm=0o700) dirname =
    let open IO.Syntax in
    let* () = S.create_parent_dir dirname perm in
    match Sys.mkdir dirname perm with
    | exception Sys_error msg -> Lwt_result.fail (`Zarr (`Write msg))
    | () -> Lwt_result.return S.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname = match Sys.is_directory dirname with
    | exception Sys_error msg -> Error (`Zarr (`Read msg))
    | false -> Error (`Zarr (`Read (Format.sprintf "%s is not a directory." dirname)))
    | true -> Ok S.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  include Zarr.Storage.Make(IO)(S)
end

module AmazonS3Store = struct
  module Credentials = Aws_s3_lwt.Credentials
  module S3 = Aws_s3_lwt.S3
  open IO.Syntax

  module S = struct
    type t =
      {retries : int
      ;bucket : string
      ;cred : Credentials.t
      ;endpoint : Aws_s3.Region.endpoint}
    type 'a io = 'a Lwt.t
    type error = [ `Request_failed of S3.error ]

    let unit_result = Lwt_result.return ()
    let fail e = Lwt_result.fail (`Zarr (`Request_failed e)) 

    let process_response ?not_found ~f = function
      | Error (S3.Not_found as e) -> Option.fold ~none:(fail e) ~some:Lwt_result.return not_found
      | r -> Result.fold ~error:fail ~ok:f r

    let process_continuation ?not_found ~stop ~more = function
      | S3.Ls.Done -> Lwt_result.return stop
      | S3.Ls.More k -> Lwt.bind (k ()) (process_response ?not_found ~f:more)

    let size t key =
      let content_size (x : S3.content) = Lwt_result.return x.size
      and bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.head ~bucket ~credentials ~key ~endpoint () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~not_found:0 ~f:content_size)

    let is_member t key = Lwt_result.map (fun s -> if s = 0 then false else true) (size t key)

    let get t key =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.get ~bucket ~credentials ~endpoint ~key () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~f:Lwt_result.return)

    let get_partial_values t key ranges =
      let add_entry acc x = Lwt.return (Result.map (List.cons x) acc) in
      let read_range t key (ofs, len) acc =
        let range = match len with
          | None -> S3.{first = Some ofs; last = None}
          | Some l -> S3.{first = Some ofs; last = Some (ofs + l - 1)}
        in
        let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
        let f ~endpoint () = S3.get ~bucket ~credentials ~endpoint ~range ~key () in
        Lwt.bind
          (S3.retry ~retries:t.retries ~endpoint ~f ())
          (process_response ~f:(add_entry acc))
      in
      Lwt_list.fold_right_s (read_range t key) ranges (Ok [])

    let set t key data =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.put ~bucket ~credentials ~endpoint ~data ~key () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~f:(fun _ -> unit_result))

    let set_partial_values t key ?(append=false) rsv =
      let* ov = Lwt_result.bind (size t key) @@ function
        | 0 -> IO.return String.empty
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

    let erase t key =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.delete ~bucket ~credentials ~endpoint ~key () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~not_found:() ~f:(fun () -> unit_result))

    let delete_content t _ S3.{key; _} = Lwt.map Fun.id (erase t key)

    let rec delete_keys t = process_continuation ~not_found:() ~stop:() ~more:(delete_all t)

    and delete_all t (xs, rest) =
      let* () = Lwt_list.fold_left_s (delete_content t) (Ok ()) xs in
      delete_keys t rest

    let erase_prefix t prefix =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint ~prefix () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~not_found:() ~f:(delete_all t))

    let content_key S3.{key; _} = key

    let rec accumulate_keys acc cont =
      let more (xs, rest) = accumulate_keys (acc @ List.map content_key xs) rest in
      process_continuation ~stop:acc ~more cont

    let list t =
      let more (xs, rest) = accumulate_keys (List.map content_key xs) rest in
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint () in
      Lwt.bind
        (S3.retry ~retries:t.retries ~endpoint ~f ())
        (process_response ~not_found:[] ~f:more)

    module M = Set.Make(String)

    let add prefix (l, r) (c : S3.content) =
      let size = String.length prefix in
      if not (String.contains_from c.key size '/') then c.key :: l, r else
      l, M.add StringLabels.(sub ~pos:0 ~len:(1 + index_from c.key size '/') c.key) r

    let rec partition_keys prefix ((l, r) as acc) cont =
      let more (xs, rest) = partition_keys prefix (List.fold_left (add prefix) acc xs) rest in
      process_continuation
        ~not_found:(l, M.elements r)
        ~stop:(l, M.elements r)
        ~more
        cont

    let list_dir t prefix =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint ~prefix () in
      let more (xs, rest) = 
        let init = List.fold_left (add prefix) ([], M.empty) xs in
        partition_keys prefix init rest
      in
      Lwt.bind (S3.retry ~retries:t.retries ~endpoint ~f ()) (process_response ~f:more)

    let rename_and_add ~t ~prefix ~new_prefix acc k =
      let l = String.length prefix in
      let k' = new_prefix ^ String.sub k l (String.length k - l) in
      let* x = get t k in
      Lwt.return (Result.map (fun xs -> (k', x) :: xs) acc)

    let rename t prefix new_prefix =
      let upload t acc (k, v) = Lwt_result.bind (set t k v) (fun () -> Lwt.return acc) in
      let remove t acc key = Lwt_result.bind (erase t key) (fun () -> Lwt.return acc) in
      let* xs = list t in
      let to_delete = List.filter (String.starts_with ~prefix) xs in
      let* new_key_data_pair = Lwt_list.fold_left_s (rename_and_add ~t ~prefix ~new_prefix) (Ok []) to_delete in
      let* () = Lwt_list.fold_left_s (upload t) (Ok ()) new_key_data_pair in
      Lwt_list.fold_left_s (remove t) (Ok ()) to_delete
  end

  let with_open ?(scheme=`Http) ?(inet=`V4) ?(retries=3) ~region ~bucket ~profile f =
    let to_s3error e = `Zarr (`Request_failed (S3.Failed e)) in
    let* cred = Lwt_result.map_error to_s3error (Credentials.Helper.get_credentials ~profile ()) in
    f S.{bucket; cred; retries; endpoint = Aws_s3.Region.endpoint ~inet ~scheme region}

  include Zarr.Storage.Make(IO)(S)
end
