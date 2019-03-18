module LazyCSV

import Mmap
import Parsers
import WeakRefStrings: WeakRefString

const CSV = LazyCSV
const DEFAULT_DELIM = ','
const DEFAULT_LINE_LEN = 2048
const DEFAULT_NUM_FIELDS = 16

function consumeBOM!(io)
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        (!eof(io) && Parsers.readbyte(io) == 0xbb) || Parsers.fastseek!(io, startpos)
        (!eof(io) && Parsers.readbyte(io) == 0xbf) || Parsers.fastseek!(io, startpos)
    end
    return
end

function read_input_data(input_source::AbstractString; kwargs...)
    #TODO this should be done async
    open(input_source, "r")
end
read_input_data(input::IOStream; kwargs...) = input

apply_mmap(input) = Mmap.mmap(input)

function read_mmap_data(input_source::Union{AbstractString,IOStream}; kwargs...)
    input = read_input_data(input_source; kwargs...)
    input_io = IOBuffer(apply_mmap(input))
    consumeBOM!(input_io)
    input_io
end

function read_mmap_data(input::IO; kwargs...)
    consumeBOM!(input)
    input
end

const IntTp = UInt64

include("buffered_vector.jl")

@noinline resize_vec!(vec::AbstractVector{T}, new_size) where {T} = resize!(vec, new_size)

const ASCII_ETB = 0x17
const ASCII_SPACE = UInt8(' ')
const ASCII_TAB = UInt8('\t')
const ASCII_RETURN = UInt8('\r')
const ASCII_NEWLINE = UInt8('\n')

"""
    ws = *(
            %x20 /              ; Space
            %x09 /              ; Horizontal tab
            %x0A /              ; Line feed or New line
            %x0D )              ; Carriage return
"""
@inline iswhitespace(c) = c == ASCII_SPACE  ||
                  c == ASCII_TAB ||
                  c == ASCII_RETURN ||
                  c == ASCII_NEWLINE

const ZERO_UINT8 = UInt8(0)
const ZERO = IntTp(0)
const ONE = IntTp(1)

function read_csv_line!(s::IO, delim::UInt8, fields::BufferedVector{WeakRefString{UInt8}},
                        buff::Vector{UInt8}, eager_parse_fields::Bool)
    buff_ptr::Ptr{UInt8} = pointer(buff)
    buff_len::IntTp = IntTp(length(buff))
    
    num_bytes_read::IntTp = ZERO
    
    fields_buff = fields.buff
    fields_len::IntTp = IntTp(length(fields_buff))
    num_fields_read::IntTp = ZERO
    prev_field_index::IntTp = ZERO
    
    while num_bytes_read == ZERO && !eof(s)
        current_char = ZERO_UINT8
        while (current_char != ASCII_NEWLINE) && !eof(s)
            current_char = read(s, UInt8)
            num_bytes_read == ZERO && iswhitespace(current_char) && continue
            num_bytes_read += ONE
            if eager_parse_fields && current_char == delim
                num_fields_read += ONE
                if num_fields_read > fields_len
                    fields_len <<= ONE
                    resize!(fields_buff, fields_len)
                end
                @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index, num_bytes_read-prev_field_index-ONE)
                prev_field_index = num_bytes_read
            end
            if num_bytes_read > buff_len
                buff_len <<= ONE
                resize_vec!(buff, buff_len)
            end
            @inbounds buff[num_bytes_read] = current_char
        end
    end
    
    if num_bytes_read > ZERO
        if eager_parse_fields
            num_fields_read += ONE
            if num_fields_read > fields_len
                fields_len <<= ONE
                resize_vec!(fields_buff, fields_len)
            end
            @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index, num_bytes_read-prev_field_index-ONE)
            fields.size = num_fields_read
        end
        WeakRefString{UInt8}(buff_ptr, num_bytes_read)
    else
        eager_parse_fields && (fields.size = ZERO)
        nothing
    end
end

# function read_csv_line!(input::IO, buff::IOBuffer)
#     read_csv_line!(input, buff.data)
# end

# import InteractiveUtils

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
    counter = 0
    
    buff = Vector{UInt8}()
    resize!(buff, DEFAULT_LINE_LEN)
    
    fields = BufferedVector{WeakRefString{UInt8}}()
    resize!(fields, DEFAULT_NUM_FIELDS)

    while (line = read_csv_line!(input_io, UInt8(delim), fields, buff, eager_parse_fields)) != nothing
        counter += 1
    end
    counter
end


end # module
