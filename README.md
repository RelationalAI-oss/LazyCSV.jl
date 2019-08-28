# LazyCSV

LazyCSV is an interface for reading CSV data in Julia programs.

The intent of this package is to provide a very fast and type-stable sequential CSV parsing that can work lazily and does not require to load the whole data into the main memory.

## Installation

Use the package manager [Pkg3](https://github.com/JuliaLang/Pkg.jl) to install `LazyCSV`. This package is not registered yet.

```bash
pkg> add https://github.com/RelationalAI-oss/LazyCSV.jl
```

## Usage

### Untyped eager mode

Parses and materializes the given CSV string or file into a `Vector{Vector{String}}`.

```julia
julia> csv_file = csvread(IOBuffer("""
         "Sell", "List", "Living", "Rooms", "Beds", "Baths", "Age", "Acres", "Taxes"
         142, 160, 28, 10, 5, 3,  60, 0.28,  3167
         175, 180, 18,  8, 4, 1,  12, 0.43,  4033
         129, 132, 13,  6, 3, 1,  41, 0.33,  1471
         138, 140, 17,  7, 3, 1,  22, 0.46,  3204
         232, 240, 25,  8, 4, 3,   5, 2.05,  3613
         135, 140, 18,  7, 4, 3,   9, 0.57,  3028
         150, 160, 20,  8, 4, 3,  18, 4.00,  3131
       """); lazy=false)
8-element Array{Array{String,1},1}:
 ["Sell", " List", " Living", " Rooms", " Beds", " Baths", " Age", " Acres", " Taxes"]
 ["142", " 160", " 28", " 10", " 5", " 3", "  60", " 0.28", "  3167"]                 
 ["175", " 180", " 18", "  8", " 4", " 1", "  12", " 0.43", "  4033"]                 
 ["129", " 132", " 13", "  6", " 3", " 1", "  41", " 0.33", "  1471"]                 
 ["138", " 140", " 17", "  7", " 3", " 1", "  22", " 0.46", "  3204"]                 
 ["232", " 240", " 25", "  8", " 4", " 3", "   5", " 2.05", "  3613"]                 
 ["135", " 140", " 18", "  7", " 4", " 3", "   9", " 0.57", "  3028"]                 
 ["150", " 160", " 20", "  8", " 4", " 3", "  18", " 4.00", "  3131"]                 
```

### Untyped lazy mode

This mode returns a `LazyCSV.File` that can be used to do a (lazy) sequential scan over
the given CSV string or file.

Here is an example that prints the input CSV string and changes the delimeter to from `,` to `|`.

```julia
using LazyCSV

julia> csv_file = csvread(IOBuffer("""
         "Sell", "List", "Living", "Rooms", "Beds", "Baths", "Age", "Acres", "Taxes"
         142, 160, 28, 10, 5, 3,  60, 0.28,  3167
         175, 180, 18,  8, 4, 1,  12, 0.43,  4033
         129, 132, 13,  6, 3, 1,  41, 0.33,  1471
         138, 140, 17,  7, 3, 1,  22, 0.46,  3204
         232, 240, 25,  8, 4, 3,   5, 2.05,  3613
         135, 140, 18,  7, 4, 3,   9, 0.57,  3028
         150, 160, 20,  8, 4, 3,  18, 4.00,  3131
       """))
File{Base.GenericIOBuffer{Array{UInt8,1}}}(IOBuffer(data=UInt8[...], readable=true, writable=false, seekable=true, append=false, size=379, maxsize=Inf, ptr=1, mark=-1), 0x2c, 0x22, 0x22, false, true, UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  …  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], LazyCSV.Record[], LazyCSV.Counter(0), LazyCSV.Counter(0))

julia> for csv_line in csv_file
         for csv_field in LazyCSV.first_rec_fields(csv_file)
           print("$csv_field|")
         end
         println()
       end
Sell| List| Living| Rooms| Beds| Baths| Age| Acres| Taxes|
142| 160| 28| 10| 5| 3|  60| 0.28|  3167|
175| 180| 18|  8| 4| 1|  12| 0.43|  4033|
129| 132| 13|  6| 3| 1|  41| 0.33|  1471|
138| 140| 17|  7| 3| 1|  22| 0.46|  3204|
232| 240| 25|  8| 4| 3|   5| 2.05|  3613|
135| 140| 18|  7| 4| 3|   9| 0.57|  3028|
150| 160| 20|  8| 4| 3|  18| 4.00|  3131|
```

### Custom (typed) lazy mode

In the lazy mode, users can use a custom `DataConsumer` that allows a fully customizable data consumption mechanism.

The custom implementation has to implement the following functions:

```julia
function consume(consumer::DataConsumer, f::File) end
function consume_header(fn::Function, consumer::DataConsumer, file::File, iter_res) end
function produce_header(pc::DataConsumer, f::File) end
function consume_rec(pc::DataConsumer, f::File, pos::FilePos, line, fields) end
function consume_rec_error(pc::DataConsumer, f::File, pos::FilePos, line_str) end
function consume_field(pc::DataConsumer, f::File, pos::FilePos, line, field_str, index::Int) end
function consume_field_error(pc::DataConsumer, f::File, pos::FilePos, line, field_str, index::Int) end
function consume_finalize(consumer::DataConsumer) end
```

You can find an example [here](https://github.com/RelationalAI-oss/LazyCSV.jl/blob/master/src/print_consumer.jl).

Note: this custom mechanism allows for proper error handling without having to stop loading the rest of records
(e.g., if a CSV record does not have all the fields, or if a field does not contain the expected data type, you
can decide to skip that row and accumulate it for reporting to the users at the end of parsing).

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
