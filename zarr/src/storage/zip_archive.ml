type error = [ `Read of string | `Write of string ]

module type S = sig
  include Storage.S with type error = error
  val open_store : ?level:Codecs.deflate_level -> string -> (t, [> `Zarr of [> `Read of string ]]) result
  (** [open_store ?level p] returns a store instance representing a zip
      archive of a Zarr v3 hierarchy stored at path [p]. [level] is the DEFLATE
      algorithm compression setting used when writing new entries into the archive. *)

  val create : ?level:Codecs.deflate_level -> string -> (t, [> `Zarr of error]) result io
  (** [create ?level p] creates a zip archive at path [p] and then returns a
      store instance representing the zip archive. [level] is the DEFLATE algorithm
      compression setting used when writing new entries into the archive. *)
end

module Make (IO : Types.IO) : S with type 'a io := 'a IO.t = struct
  open IO.Infix
  open IO.Syntax

  module Store = struct
    type 'a io = 'a IO.t
    type t = {path : string; level : int}
    type nonrec error = error

    let with_open_in path f = match Zip.open_in path with
      | exception Zip.Error (_, entry, msg) ->
        IO.error (`Zarr (`Read (Printf.sprintf "%s: %s" entry msg)))
      | ic ->
        let out = f ic in
        Zip.close_in ic;
        IO.lift out

    let with_open_out path f = match Zip.open_update path with
      | exception Zip.Error (_, entry, msg) ->
        IO.error (`Zarr (`Write (Printf.sprintf "%s: %s" entry msg)))
      | oc ->
        let out = f oc in
        Zip.close_out oc;
        IO.lift out

    let read_entry ~ic ~f e = match Zip.read_entry ic e with
      | exception Zip.Error (_, entry, msg) ->
        Error (`Zarr (`Read (Printf.sprintf "%s: %s" entry msg)))
      | s -> f s

    let write_entry ~level ~key data oc = match Zip.add_entry ~level data oc key with
      | exception Zip.Error (_, entry, msg) ->
        Error (`Zarr (`Write (Printf.sprintf "%s: %s" entry msg)))
      | () as x -> Ok x

    let is_member t key =
      let entry_exists ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> Ok false
        | _ -> Ok true
      in 
      with_open_in t.path (entry_exists ~key)

    let size t key =
      let entry_size ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> Ok 0
        | e -> Ok e.uncompressed_size
      in
      with_open_in t.path (entry_size ~key)

    let get t key =
      let read_entry ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> Error (`Zarr (`Read (Printf.sprintf "%s: does not not exist" key)))
        | e -> read_entry ~ic ~f:Result.ok e
      in
      with_open_in t.path (read_entry ~key)

    (* TODO: Maybe account for String.sub possibly throwing Invalid_argument exception?
      But then the way this function is used in codecs.ml it ensures that we pass valid
      substring args always.*)
    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | Some l -> String.sub data ofs l
        | None -> String.sub data ofs (size - ofs)
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let list t =
      let get_keys ic = Ok (List.map (fun (e : Zip.entry) -> e.filename) (Zip.entries ic)) in
      with_open_in t.path get_keys

    module StrSet = Set.Make(String)

    let list_dir t prefix =
      let n = String.length prefix in
      let add_entry_with_prefix ((l, r) as acc) = function
        | (e : Zip.entry) when not (String.starts_with ~prefix e.filename) -> acc
        | e when String.contains_from e.filename n '/' ->
          let key = e.filename in
          let pre = String.sub key 0 (1 + String.index_from key n '/') in
          StrSet.add pre l, r
        | e -> l, e.filename :: r
      in
      let+ entries = with_open_in t.path (fun ic -> Ok (Zip.entries ic)) in
      let prefs, keys = List.fold_left add_entry_with_prefix (StrSet.empty, []) entries in
      keys, StrSet.elements prefs

    let set t key data = with_open_out t.path (write_entry ~level:t.level ~key data)

    let set_partial_values t key ?(append=false) rvs =
      let* ov = size t key >>= function
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
      set t key (List.fold_left f ov rvs)

    let add_to_zip ~oc ~level acc (key, data) =
      Result.bind acc (fun () -> write_entry ~level ~key data oc)

    let rename t prefix new_prefix =
      let accumulate ~prefix ~new_prefix acc path data =
        if not (String.starts_with ~prefix path) then Ok ((path, data) :: acc) else
        let l = String.length prefix in
        let path' = new_prefix ^ String.sub path l (String.length path - l) in
        Ok ((path', data) :: acc)
      in
      let add_pair ~ic ~prefix ~new_prefix acc (entry : Zip.entry) =
        Result.bind acc (fun k -> read_entry ~ic ~f:(accumulate ~prefix ~new_prefix k entry.filename) entry)
      in
      let rename_entries ic = List.fold_left (add_pair ~ic ~prefix ~new_prefix) (Ok []) (Zip.entries ic) in
      let* pairs = with_open_in t.path rename_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path (fun oc -> List.fold_left (add_to_zip ~oc ~level:t.level) (Ok ()) pairs)

    let prepend_path ~acc path data = Result.map (List.cons (path, data)) acc

    let erase t key =
      let filter ~ic acc = function
        | (e : Zip.entry) when e.filename = key -> acc
        | e -> read_entry ~ic ~f:(prepend_path ~acc e.filename) e
      in
      let filter_entries ic = List.fold_left (filter ~ic) (Ok []) (Zip.entries ic) in
      let* pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path (fun oc -> List.fold_left (add_to_zip ~oc ~level:t.level) (Ok ()) pairs)

    let erase_prefix t prefix =
      let filter ~ic ~prefix acc = function
        | (e : Zip.entry) when String.starts_with ~prefix e.filename -> acc
        | e -> read_entry ~ic ~f:(prepend_path ~acc e.filename) e
      in
      let filter_entries ic = List.fold_left (filter ~ic ~prefix) (Ok []) (Zip.entries ic) in
      let* pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path (fun oc -> List.fold_left (add_to_zip ~oc ~level:t.level) (Ok ()) pairs)
  end

  let open_store ?(level=Codecs.L6) path =
    let l = match level with
      | L0 -> 0 | L1 -> 1 | L2 -> 2 | L3 -> 3 | L4 -> 4
      | L5 -> 5 | L6 -> 6 | L7 -> 7 | L8 -> 8 | L9 -> 9
    in
    if Sys.file_exists path then Ok Store.{path; level = l} else
    Error (`Zarr (`Read (Printf.sprintf "%s: File does not exist." path)))

  let create ?(level=Codecs.L6) path =
    if Sys.file_exists path
    then IO.error (`Zarr (`Read (Printf.sprintf "%s: File already exists." path))) else
    let oc = Zip.open_out path in
    Zip.close_out oc;
    IO.lift (open_store ~level path)

  include Storage.Make(IO)(Store)
end
