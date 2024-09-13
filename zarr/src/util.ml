module ArrayMap = struct
  include Map.Make (struct
    type t = int array
    let compare (x : t) (y : t) = Stdlib.compare x y
  end)

  let add_to_list k v map =
    update k (Option.fold ~none:(Some [v]) ~some:(fun l -> Some (v :: l))) map
end

module Result_syntax = struct
  let (let*) = Result.bind
  let (let+) x f = Result.map f x
end

let get_name j =
  Yojson.Safe.Util.(member "name" j |> to_string)

let prod x =
  Array.fold_left Int.mul 1 x

let max = Array.fold_left Int.max Int.min_int

(* Obtained from: https://discuss.ocaml.org/t/how-to-create-a-new-file-while-automatically-creating-any-intermediate-directories/14837/5?u=zoj613 *)
let rec create_parent_dir fn perm =
  let parent_dir = Filename.dirname fn in
  if not (Sys.file_exists parent_dir) then begin
    create_parent_dir parent_dir perm;
    Sys.mkdir parent_dir perm
  end

let sanitize_dir dir =
  Option.fold ~none:dir ~some:Fun.id @@ Filename.chop_suffix_opt ~suffix:"/" dir
