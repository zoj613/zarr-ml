module StrMap = Util.StrMap

let create () = StrMap.create 16

module Impl = struct
  type t = string StrMap.t

  let get t key =
    Option.to_result
      ~none:(`Store_read (key ^ " not found.")) @@
      StrMap.find_opt t key

  let set t key value =
    StrMap.replace t key value

  let list t =
    StrMap.to_seq_keys t |> List.of_seq

  let is_member = StrMap.mem

  let erase = StrMap.remove

  let size t key =
    get t key |> Result.get_ok |> String.length

  let erase_prefix t pre =
    StrMap.filter_map_inplace
      (fun k v ->
        if String.starts_with ~prefix:pre k then None else Some v) t

  let get_partial_values t key ranges =
    Storage_intf.Base.get_partial_values
      ~get_fn:get t key ranges

  let set_partial_values t key ?(append=false) rv =
    Storage_intf.Base.set_partial_values
      ~set_fn:set ~get_fn:get t key append rv

  let list_prefix t pre =
    Storage_intf.Base.list_prefix ~list_fn:list t pre

  let list_dir t pre =
    Storage_intf.Base.list_dir ~list_fn:list t pre
end
