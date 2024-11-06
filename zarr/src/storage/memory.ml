module type S = sig
  include Storage.STORE
  val create : unit -> t
  (** [create ()] returns a new In-memory Zarr store type.*)
end

module Make (Deferred : Types.Deferred) = struct
  module M = Map.Make(String)
  module Deferred = Deferred
  open Deferred.Syntax

  type t = string M.t Atomic.t

  let create : unit -> t = fun () -> Atomic.make M.empty

  let get t key = match M.find_opt key (Atomic.get (t : t)) with
    | None -> raise (Storage.Key_not_found key)
    | Some v -> Deferred.return v

  let rec set t key value =
    let m = Atomic.get (t : t) in
    if Atomic.compare_and_set t m (M.add key value m)
    then Deferred.return_unit else set t key value 

  let list t =
    Deferred.return @@ M.fold (fun k _ acc -> k :: acc) (Atomic.get (t : t)) []

  let is_member t key = Deferred.return @@ M.mem key (Atomic.get (t : t))

  let rec erase t key =
    let m = Atomic.get (t : t) in
    let m' = M.update key (Fun.const None) m in
    if Atomic.compare_and_set t m m'
    then Deferred.return_unit else erase t key

  let size t key =
    let binding_opt = M.find_opt key (Atomic.get (t : t)) in
    Deferred.return (Option.fold ~none:0 ~some:String.length binding_opt)

  let rec erase_prefix t prefix =
    let pred ~prefix k v = if String.starts_with ~prefix k then None else Some v in
    let m = Atomic.get (t : t) in
    let m' = M.filter_map (pred ~prefix) m in
    if Atomic.compare_and_set t m m'
    then Deferred.return_unit else erase_prefix t prefix

  let get_partial_values t key ranges =
    let read_range ~data ~size (ofs, len) = match len with
      | Some l -> String.sub data ofs l
      | None -> String.sub data ofs (size - ofs)
    in
    let+ data = get t key in
    let size = String.length data in
    List.map (read_range ~data ~size) ranges

  let rec set_partial_values t key ?(append=false) rv =
    let m = Atomic.get (t : t) in
    let ov = Option.fold ~none:String.empty ~some:Fun.id (M.find_opt key m) in
    let f = if append || ov = String.empty then
      fun acc (_, v) -> acc ^ v else
      fun acc (rs, v) ->
        let s = Bytes.unsafe_of_string acc in
        Bytes.blit_string v 0 s rs String.(length v);
        Bytes.unsafe_to_string s
    in
    let m' = M.add key (List.fold_left f ov rv) m in
    if Atomic.compare_and_set t m m'
    then Deferred.return_unit else set_partial_values t key ~append rv

  let list_dir t prefix =
    let module S = Set.Make(String) in
    let m = Atomic.get (t : t) in
    let add ~size ~prefix key _ ((l, r) as acc) =
      if not (String.starts_with ~prefix key) then acc else
      if not (String.contains_from key size '/') then key :: l, r else
      l, S.add String.(sub key 0 @@ 1 + index_from key size '/') r
    in
    let size = String.length prefix in
    let keys, prefixes = M.fold (add ~prefix ~size) m ([], S.empty) in
    Deferred.return (keys, S.elements prefixes)

  let rec rename t prefix new_prefix =
    let add ~prefix ~new_prefix k v acc =
      if not (String.starts_with ~prefix k) then M.add k v acc else
      let l = String.length prefix in
      let k' = new_prefix ^ String.sub k l (String.length k - l) in
      M.add k' v acc
    in
    let m = Atomic.get (t : t) in
    let m' = M.fold (add ~prefix ~new_prefix) m M.empty in
    if Atomic.compare_and_set t m m'
    then Deferred.return_unit else rename t prefix new_prefix
end
