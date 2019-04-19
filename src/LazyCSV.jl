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
include("consumer.jl")

const ZERO_UINT8 = UInt8(0)
const ZERO = IntTp(0)
const ONE = IntTp(1)

const DEFAULT_HEADER_EXISTS = false

"""
    read_csv_line!(f::File, eager_parse_fields::Bool)

reads a single line of input and returns it. Please note that the returned string is a
`WeakRefString`.

In addition, if `eager_parse_fields` is true, it will also accumulate the fields (as a
vector of `WeakRefString`) into the `fields_buff` in the file.
"""
function read_csv_line!(f::File, eager_parse_fields::Bool=f.eager_parse_fields)
    read_csv_line!(f.input, f.line_buff, f.delim, f.quotechar, f.escapechar, f.fields_buff,
                   eager_parse_fields, f.current_byte_pos)
end

"""
This function is at the hear of LazyCSV and it's actually an inlined parser-combinator
that know how to parse a line of a CSV source.

It currently supports specifying:
  - `delim`: CSV delimiter character
  - `quotechar`: CSV quote character
  - `escapechar`: CSV escape character for escaping the quote and delimiter characters

In addition, by passing `eager_parse_fields=true`, then all the fields are also parsed
and stored as separate `WeakRefString`s. Otherwise, if `eager_parse_fields=false`, then
fields are not parsed and only a single line is extracted from the input (as a
`WeakRefString`).

This function returns a `Tuple{FilePos, WeakRefString{UInt8}}`. The first element of
this tuple contains the byte position of the starting point of this record (in the
source) and the second element is the record line content.

Note: the logic is a little bit complex, but being a core function, it's intended to
      be the most efficient code one can write. Any optimization is welcome!
"""
function read_csv_line!(s::IO, buff::Vector{UInt8}, delim::UInt8, quotechar::UInt8,
                        escapechar::UInt8, fields::BufferedVector{WeakRefString{UInt8}},
                        eager_parse_fields::Bool, pos::MutFilePos)
    prev_pos = pos.v
    current_pos = prev_pos
    # take the pointer to buffer only once
    buff_ptr::Ptr{UInt8} = pointer(buff)
    # cache the buffer length
    buff_len::IntTp = IntTp(length(buff))
    
    # accumulates the number of bytes read for the current line
    # (it won't count the leading whitespaces)
    num_bytes_read::IntTp = ZERO
    
    # the buffer for storing fields (if `eager_parse_fields` is true)
    fields_buff = fields.buff
    # cache the length of fields buffer
    fields_len::IntTp = IntTp(length(fields_buff))
    # accumulates the number of fields read in this line (if `eager_parse_fields` is true)
    num_fields_read::IntTp = ZERO
    # stores the index of the separator for the previous field.
    prev_field_index::IntTp = ZERO

    # `inside_quote` determines whether the given fields is quoted and we are scanning
    # inside the quote this variable is initialized here, as there might be leading
    # whitespaces inside the quote
    inside_quote::Bool = false
    
    same_quote_and_escape::Bool = quotechar == escapechar
    
    # this first while-loop is for bypassing the empty lines or the lines with only whitespaces
    while num_bytes_read == ZERO && !eof(s)
        # stores the next byte value
        # it's important that in the UTF-8 format, all non-ASCII characters start with 1-bit
        # on the leftmost (in all their parts) and it makes it easy to find special characters
        # (e.g., new-line or ASCII delimiters) using the byte value.
        current_char::UInt8 = ZERO_UINT8
        
        # we look for a new-line character or enf-of-file
        while (current_char != ASCII_NEWLINE && current_char != ASCII_RETURN) && !eof(s)
            prev_char::UInt8 = current_char
            current_char = Parsers.readbyte(s)
            current_pos += 1
            
            # skip the leading whitespaces if not inside a quote
            num_bytes_read == ZERO && !inside_quote && iswhitespace(current_char) && continue
            
            # handles the quote character
            # there are two cases:
            if same_quote_and_escape
                # case 1: if both quotechar and escapechar are the same
                if current_char == quotechar
                    if inside_quote
                        # if we are already inside a quote, then this is either an end-quote or
                        # a escape character and we have to escape it anyways
                        inside_quote = false
                        continue
                    else
                        inside_quote = true
                        # if we were not in a quote, it could be because of a escape quote
                        # before this one and the following check does the job. If the prev.
                        # character was also a `quotechar`, then we have to write the character.
                        prev_char != quotechar && continue
                    end
                end
            else
                # case 2: if quotechar and escapechar are different
                if current_char == quotechar
                    if prev_char != escapechar
                        inside_quote = !inside_quote
                        continue
                    end
                elseif current_char == escapechar
                    if prev_char != escapechar
                        continue
                    end
                end
            end

            num_bytes_read += ONE
            if eager_parse_fields && !inside_quote && current_char == delim
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
                buff_ptr = pointer(buff) #resizing the buffer might change its location on heap
            end
            @inbounds buff[num_bytes_read] = current_char
        end
    end
    
    if num_bytes_read > ZERO
        # handle the last field if anything is read at all
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
            # correctly set the number of fields read to `num_fields_read`
            fields.size = num_fields_read
        end
        pos.v = current_pos
        (FilePos(prev_pos), WeakRefString{UInt8}(buff_ptr, num_bytes_read))
    else
        # if fields were supposed to be parsed eagerly, then correctly set the size of buffer to zero
        eager_parse_fields && (fields.size = ZERO)
        # if nothing is read, then return `nothing`
        nothing
    end
end

"""
    csvread(input::IO, delim=','; <arguments>...)
Read CSV from `file`. Returns a tuple of 2 elements:
1. A tuple of columns each either a `Vector`, or `StringArray`
2. column names if `header_exists=true`, empty array otherwise
# Arguments:
- `input`: an input file path or an IO object
- `delim`: the delimiter character
- `lazy`: if `true`, then an instance of `LazyCSV.File` is returned.
          Otherwise, if `false`, the result gets materialized and then returned.
- `quotechar`: character used to quote strings, defaults to `"`
- `escapechar`: character used to escape quotechar in strings. (could be the same as quotechar)
- `header_exists`: boolean specifying whether CSV file contains a header
"""
function csvread(input::Union{IO,AbstractString}; delim::Union{Char, Nothing}=nothing,
                 lazy=true, eager_parse_fields=DEFAULT_EAGER_PARSE_FIELDS,
                 quotechar::Char=DEFAULT_QUOTE, escapechar::Char=quotechar,
                 header_exists::Bool=DEFAULT_HEADER_EXISTS, kw...)
    input_io = read_mmap_data(input)

    if delim === nothing
        if isa(input, AbstractString)
            if endswith(input, ".tsv")
                delim = '\t'
            elseif endswith(input, ".wsv")
                delim = ' '
            elseif endswith(input, ".tbl")
                delim = '|'
            else
                delim = ','
            end
        else
            delim = ','
        end
    end

    file = File(input_io; delim=delim, eager_parse_fields=eager_parse_fields,
                quotechar=quotechar, escapechar=escapechar, header_exists=header_exists,
                line_buff_len=DEFAULT_LINE_LEN, fields_buff_len=DEFAULT_NUM_FIELDS)
    
    lazy ? file : materialize(file)
end

export csvread

end # module
