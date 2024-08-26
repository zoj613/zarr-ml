type 'a t = 'a
let return = Fun.id
let return_unit = ()
let iter = List.iter
let fold_left = List.fold_left
let map = List.map
let concat_map = List.concat_map

module Infix = struct
  let (>>=) x f = f x
  let (>>|) = (>>=)
end
