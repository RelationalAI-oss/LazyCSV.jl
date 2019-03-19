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

function read_csv_line!(s::IO, buff::Vector{UInt8}, delim::UInt8,
                        fields::BufferedVector{WeakRefString{UInt8}}, eager_parse_fields::Bool)
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
                @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index, 
                                                                              num_bytes_read-prev_field_index-ONE)
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
            @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index,
                                                                          num_bytes_read-prev_field_index-ONE)
            fields.size = num_fields_read
        end
        WeakRefString{UInt8}(buff_ptr, num_bytes_read)
    else
        eager_parse_fields && (fields.size = ZERO)
        nothing
    end
end

mutable struct Counter
    v::Int
end

struct File{IO_TYPE}
    input::IO_TYPE
    delim::UInt8
    eager_parse_fields::Bool
    line_buff::Vector{UInt8}
    fields_buff::BufferedVector{WeakRefString{UInt8}}
    current_line::Counter
    function File(input::IO_TYPE, delim::UInt8,
                  eager_parse_fields::Bool, line_buff::Vector{UInt8},
                  fields_buff::BufferedVector{WeakRefString{UInt8}}) where {IO_TYPE}
        new{IO_TYPE}(input, delim, eager_parse_fields, line_buff, fields_buff, Counter(0))
    end
end

function File(input::IO, delim::Char, eager_parse_fields::Bool)
    buff = Vector{UInt8}()
    resize!(buff, DEFAULT_LINE_LEN)

    fields = BufferedVector{WeakRefString{UInt8}}()
    resize!(fields, DEFAULT_NUM_FIELDS)

    File(input, UInt8(delim), eager_parse_fields, buff, fields)
end

function read_csv_line!(Base.@nospecialize(s::IO), buff::IOBuffer, delim::UInt8,
                        fields::BufferedVector{WeakRefString{UInt8}}, eager_parse_fields::Bool)
    read_csv_line!(s, buff.data, delim, fields, eager_parse_fields)
end

function read_csv_line!(f::File, eager_parse_fields::Bool=f.eager_parse_fields)
    read_csv_line!(f.input, f.line_buff, f.delim, f.fields_buff, eager_parse_fields)
end

const DO_NOT_PARSE_LINE_EAGERLY = false

function Base.iterate(f::File, state::Int = 1)
    if f.current_line.v + 1 == state
        next_line = read_csv_line!(f)
        if next_line === nothing
            nothing
        else
            f.current_line.v += 1
            (state+1, state+1)
        end
    # elseif f.current_line.v < state
    #     while f.current_line.v + 1 != state
    #         read_csv_line!(f, DO_NOT_PARSE_LINE_EAGERLY)
    #         f.current_line.v += 1
    #     end
    #     iterate(f, state)
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
