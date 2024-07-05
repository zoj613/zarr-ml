module HashableString = struct
  type t = string
  let hash = Hashtbl.hash
  let equal = String.equal
end

module StrMap = Hashtbl.Make (HashableString)

let create () = StrMap.create 16

module Impl = struct
  type t = string StrMap.t

  let get t key =
    Option.to_result
      ~none:(`Store_read key) @@ StrMap.find_opt t key

  let set t key value =
    StrMap.replace t key value

  let list t =
    StrMap.to_seq_keys t |> List.of_seq

  let is_member = StrMap.mem

  let erase = StrMap.remove

  let erase_prefix t pre =
    StrMap.filter_map_inplace
      (fun k v ->
        if String.starts_with ~prefix:pre k then 
          None
        else 
          Some v) t

  let get_partial_values t kr_pairs =
    Storage_intf.Base.get_partial_values
      ~get_fn:get t kr_pairs

  let set_partial_values t krv_triplet =
    Storage_intf.Base.set_partial_values
      ~set_fn:set ~get_fn:get t krv_triplet

  let erase_values t keys =
    Storage_intf.Base.erase_values ~erase_fn:erase t keys

  let list_prefix pre t =
    Storage_intf.Base.list_prefix ~list_fn:list t pre

  let list_dir t pre =
    Storage_intf.Base.list_dir ~list_fn:list t pre
end