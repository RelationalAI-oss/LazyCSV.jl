module LazyCSV

import Mmap
import Parsers
import WeakRefStrings: WeakRefString

const CSV = LazyCSV

include("io.jl")
include("util.jl")
const IntTp = UInt64
include("buffered_vector.jl")
include("file.jl")

const ZERO_UINT8 = UInt8(0)
const ZERO = IntTp(0)
const ONE = IntTp(1)

"""
    read_csv_line!(f::File, eager_parse_fields::Bool)

reads a single line of input and returns it. Please note that the returned string is a
`WeakRefString`.

In addition, if `eager_parse_fields` is true, it will also accumulate the fields (as a
vector of `WeakRefString`) into the `fields_buff` in the file.
"""
function read_csv_line!(f::File, eager_parse_fields::Bool=f.eager_parse_fields)
    read_csv_line!(f.input, f.line_buff, f.delim, f.fields_buff, eager_parse_fields)
end

function read_csv_line!(s::IO, buff::IOBuffer, delim::UInt8,
                        fields::BufferedVector{WeakRefString{UInt8}}, eager_parse_fields::Bool)
    read_csv_line!(s, buff.data, delim, fields, eager_parse_fields)
end

function read_csv_line!(s::IO, buff::Vector{UInt8}, delim::UInt8,
                        fields::BufferedVector{WeakRefString{UInt8}}, eager_parse_fields::Bool)
    # take the pointer to buffer only once
    buff_ptr::Ptr{UInt8} = pointer(buff)
    # cache the buffer length
    buff_len::IntTp = IntTp(length(buff))
    
    # accumulates the number of bytes read for the current line (it won't count the leading whitespaces)
    num_bytes_read::IntTp = ZERO
    
    # the buffer for storing fields (if `eager_parse_fields` is true)
    fields_buff = fields.buff
    # cache the length of fields buffer
    fields_len::IntTp = IntTp(length(fields_buff))
    # accumulates the number of fields read in this line (if `eager_parse_fields` is true)
    num_fields_read::IntTp = ZERO
    # stores the index of the separator for the previous field.
    prev_field_index::IntTp = ZERO
    
    # this first while-loop is for bypassing the empty lines or the lines with only whitespaces
    while num_bytes_read == ZERO && !eof(s)
        # stores the next byte value
        # it's important that in the UTF-8 format, all non-ASCII characters start with 1-bit
        # on the leftmost (in all their parts) and it makes it easy to find special characters
        # (e.g., new-line or ASCII delimiters) using the byte value.
        current_char = ZERO_UINT8
        
        # we look for a new-line character or enf-of-file
        while (current_char != ASCII_NEWLINE) && !eof(s)
            current_char = read(s, UInt8)
            
            # skip the leading whitespaces
            num_bytes_read == ZERO && iswhitespace(current_char) && continue
            
            num_bytes_read += ONE
            if eager_parse_fields && current_char == delim
                ########## START ADD FIELD LOGIC (copied below) #######
                num_fields_read += ONE
                if num_fields_read > fields_len
                    # extend the fields buffer if required
                    fields_len <<= ONE
                    resize_vec!(fields_buff, fields_len)
                end
                @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index,
                                                                              num_bytes_read-prev_field_index-ONE)
                ########## FINISH ADD FIELD LOGIC #####################
                prev_field_index = num_bytes_read
            end
            if num_bytes_read > buff_len
                # extend the line buffer if required
                buff_len <<= ONE
                resize_vec!(buff, buff_len)
            end
            @inbounds buff[num_bytes_read] = current_char
        end
    end
    
    if num_bytes_read > ZERO
        if eager_parse_fields
            ########## START ADD FIELD LOGIC (copied above) #######
            num_fields_read += ONE
            if num_fields_read > fields_len
                # extend the fields buffer if required
                fields_len <<= ONE
                resize_vec!(fields_buff, fields_len)
            end
            @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index,
                                                                          num_bytes_read-prev_field_index-ONE)
            ########## FINISH ADD FIELD LOGIC #####################
            fields.size = num_fields_read
        end
        WeakRefString{UInt8}(buff_ptr, num_bytes_read)
    else
        eager_parse_fields && (fields.size = ZERO)
        nothing
    end
end

function Base.iterate(f::File, state::Int = 1)
    if f.current_line.v + 1 == state
        next_line = read_csv_line!(f)
        if next_line === nothing
            nothing
        else
            f.current_line.v += 1
            (state+1, state+1)
        end
    else
        #TODO: shall we throw an error here?
        # we are expecting the user to always scan sequentially
        nothing
    end
end

result_line_collection(f::File) = Vector{String}()
result_collection(f::File) = Vector{Vector{String}}()

function materialize_line(f::File, line)
    line_vec = result_line_collection(f)
    for field in f.fields_buff
        push!(line_vec, string(field))
    end
    line_vec
end

function materialize(f::File, vec)
    counter = 0
    while (line = read_csv_line!(f)) != nothing
        push!(vec, materialize_line(f, line))
        counter += 1
    end
    f.current_line.v = counter
    nothing
end

function materialize(f::File)
    materialize(f, result_collection(f))
end

"""
    csvread(input::IO, delim=','; <arguments>...)
Read CSV from `file`. Returns a tuple of 2 elements:
1. A tuple of columns each either a `Vector`, or `StringArray`
2. column names if `header_exists=true`, empty array otherwise
# Arguments:
- `input`: an IO object
- `delim`: the delimiter character
- `spacedelim`: (Bool) parse space-delimited files. `delim` has no effect if true.
- `quotechar`: character used to quote strings, defaults to `"`
- `escapechar`: character used to escape quotechar in strings. (could be the same as quotechar)
- `commentchar`: ignore lines that begin with commentchar
- `nrows`: number of rows in the file. Defaults to `0` in which case we try to estimate this.
- `skiplines_begin`: skips specified number of lines at the beginning of the file
- `header_exists`: boolean specifying whether CSV file contains a header
- `nastrings`: strings that are to be considered NA. Defaults to `TextParse.NA_STRINGS`
- `colnames`: manually specified column names. Could be a vector or a dictionary from Int index (the column) to String column name.
- `colparsers`: Parsers to use for specified columns. This can be a vector or a dictionary from column name / column index (Int) to a "parser". The simplest parser is a type such as Int, Float64. It can also be a `dateformat"..."`, see [CustomParser](@ref) if you want to plug in custom parsing behavior
- `type_detect_rows`: number of rows to use to infer the initial `colparsers` defaults to 20.
"""
function csvread(input::Union{IO,AbstractString}, delim::Char=DEFAULT_DELIM; lazy=true, eager_parse_fields=true, kw...)
    input_io = read_mmap_data(input)

    file = File(input_io, delim, eager_parse_fields)
    
    lazy ? file : materialize(file)
end


end # module
