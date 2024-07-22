(lang dune 3.15)

(name zarr)

(version 0.1.0)

(generate_opam_files true)

(source
 (github zoj613/zarr-ml))

(authors "Zolisa Bleki")

(maintainers "Zolisa Bleki")

(license BSD-3-Clause)

(documentation https://zoj613.github.io/zarr-ml)

(package
 (name zarr)
 (synopsis "An Ocaml implementation of the  Zarr V3 specification.")
 (description "A longer description")
 (depends
   dune
   (ocaml (>= 4.14.2))
   yojson
   ezgzip
   owl
   stdint
   checkseum
   (odoc :with-doc)
   (ounit2 :with-test)
   (ppx_deriving :with-test)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test)))
 (tags
  ("zarr" "chunked arrays" "zarr version 3")))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
