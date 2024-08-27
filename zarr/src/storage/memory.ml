module StrMap = Map.Make(struct
  type t = string
  let compare = String.compare
end)

let create () = Atomic.make StrMap.empty

module Make (Deferred : Types.Deferred) = struct
  module Deferred = Deferred
  open Deferred.Infix

  type t = string StrMap.t Atomic.t

  let get t key =
    match StrMap.find_opt key @@ Atomic.get t with
    | Some v -> Deferred.return v
    | None -> raise @@ Storage.Key_not_found key

  let rec set t key value =
    let m = Atomic.get t in
    let m' = StrMap.add key value m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set t key value else Deferred.return_unit

  let list t =
    Deferred.return @@ fst @@ List.split @@ StrMap.bindings @@ Atomic.get t

  let is_member t key =
    Deferred.return @@ StrMap.mem key @@ Atomic.get t

  let rec erase t key =
    let m = Atomic.get t in
    let m' = StrMap.update key (Fun.const None) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase t key else Deferred.return_unit

  let size t key =
    Deferred.return @@ String.length @@ StrMap.find key @@ Atomic.get t

  let rec erase_prefix t pre =
    let m = Atomic.get t in
    let m' = 
      StrMap.filter_map
        (fun k v ->
          if String.starts_with ~prefix:pre k then None else Some v) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase_prefix t pre else Deferred.return_unit

  let get_partial_values t key ranges =
    get t key >>= fun v ->
    let size = String.length v in
    Deferred.fold_left
      (fun acc (rs, len) ->
        Deferred.return
        (match len with
        | None -> String.sub v rs (size - rs) :: acc
        | Some l -> String.sub v rs l :: acc)) [] @@ List.rev ranges

  let rec set_partial_values t key ?(append=false) rv =
    let f = 
      if append then
        fun acc (_, v) ->
          Deferred.return @@ acc ^ v
      else
        fun acc (rs, v) ->
          let s = Bytes.of_string acc in
          String.(length v |> Bytes.blit_string v 0 s rs);
          Deferred.return @@ Bytes.to_string s
    in
    let m = Atomic.get t in
    let ov = StrMap.find key m in
    Deferred.fold_left f ov rv >>= fun nv ->
    let m' = StrMap.add key nv m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set_partial_values t key ~append rv
    else Deferred.return_unit

  let list_dir t prefix =
    let module StrSet = Util.StrSet in
    list t >>= fun xs ->
    let n = String.length prefix in
    Deferred.fold_left
      (fun ((l, r) as a) key ->
        let pred = String.starts_with ~prefix key in
        match key with
        | k when pred && String.contains_from k n '/' ->
          let l' = StrSet.add String.(sub k 0 @@ 1 + index_from k n '/') l in
          Deferred.return (l', r)
        | k when pred -> Deferred.return (l, k :: r)
        | _ -> Deferred.return a) (StrSet.empty, []) xs
    >>| fun (prefs, keys) ->
    keys, StrSet.elements prefs
end
