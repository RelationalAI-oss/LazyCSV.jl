module LazyCSV

import Mmap
import Parsers
import WeakRefStrings: WeakRefString

include("io.jl")
include("util.jl")

const SizeType = UInt64
const ZERO_UINT8 = UInt8(0)
const ZERO = zero(SizeType)
const ONE = one(SizeType)

include("buffered_vector.jl")
include("file.jl")
include("consumer.jl")

# the default value that determines whether the CSV parser should assume that the CSV file
# has a header not. Currently, by default it's assumed that there's no header in the CSV
# file if not otherwise stated.
const DEFAULT_HEADER_EXISTS = false

"""
    read_csv_line!(f::File)

read/parses a single line of input and returns it.

If no line is remained, `nothing` will be returned.
Otherwise, a pair of file position (`FilePos`) and line string is returned.

Please note that the returned line string is a `WeakRefString`.
"""
function read_csv_line!(f::File)::Union{Nothing, Tuple{FilePos, WeakRefString{UInt8}}}
    next_line_start_byte_pos_in_lines_buff = ZERO
    empty!(f.recs)
    resize!(f.recs, ONE)
    res = read_csv_line!(f.input, f.lines_buff, next_line_start_byte_pos_in_lines_buff, f.delim,
                   f.quotechar, f.escapechar, first_rec_fields(f), f.eager_parse_fields,
                   f.current_byte_pos)
    if res === nothing
        nothing
    else
        (completed_line, pos, line, _) = res
        @assert completed_line
        current_rec = first_rec(f)
        current_rec.pos = pos
        current_rec.line = line
        f.recs.size = ONE
        (pos, line)
    end
end

"""
A helper function to read/parse multiple lines of CSV files.
"""
function read_csv_lines!(f::File)
    # the byte index where the next line starts in the buffer
    next_line_start_byte_pos_in_lines_buff::SizeType = ZERO
    # number of lines read until now
    num_lines::SizeType = ZERO
    
    # clear out all the previous records
    empty!(f.recs)
    # the maximum number of lines to read/parse together
    max_lines = capacity(f.recs)
    
    # a flag that indicates whether we ran out of buffer in the middle of a line
    has_uncompleted_line = false
    
    while true
    	# if we read enough lines, we can stop for now
        num_lines >= max_lines && break
        
        # this will be the next line
        num_lines += ONE
        current_rec = f.recs[num_lines]
        res = read_csv_line!(f.input, f.lines_buff, next_line_start_byte_pos_in_lines_buff, f.delim,
                       f.quotechar, f.escapechar, current_rec.fields_buff, f.eager_parse_fields,
                       f.current_byte_pos)
        if res === nothing
        	# if there's no more line to read, we have to decrement the line counter that
        	# we have already incremented before.
            num_lines -= ONE
            break
        end
        (completed_line, pos, line, bytes_read) = res
        if completed_line
        	# if we were able to read a full line, then we can update the record information
        	# and prepare for reading the next line
            current_rec.pos = pos
            current_rec.line = line
            next_line_start_byte_pos_in_lines_buff += bytes_read
        else
        	# if we ran out of buffer in the middle of the run, we should stop for now and
        	# finish the line in the next batch
            has_uncompleted_line = true
            # this is done by reseting the input IO to the beginning of half-read line
            Parsers.fastseek!(f.input, pos.v)
            num_lines -= ONE
            break
        end
    end
    # fix the number of records
    f.recs.size = num_lines
    return has_uncompleted_line
end

@inline function private_add_field(num_fields_read, fields_len, fields_buff, buff_ptr, prev_field_index, num_bytes_read)
    num_fields_read += ONE
    if num_fields_read > fields_len
        # extend the fields buffer if required
        fields_len <<= ONE
        resize_vec!(fields_buff, fields_len)
    end
    @inbounds fields_buff[num_fields_read] = WeakRefString{UInt8}(buff_ptr+prev_field_index,
                                                                  num_bytes_read-prev_field_index-ONE)
    (num_fields_read, fields_len)
end

is_end_of_line_char(current_char::UInt8) = current_char == ASCII_NEWLINE ||
                                           current_char == ASCII_RETURN

"""
This function is at the heart of LazyCSV and it's actually an inlined parser-combinator
that knows how to parse a line of a CSV source.

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
function read_csv_line!(s::IO, buff::Vector{UInt8}, buff_start::SizeType, delim::UInt8,
                        quotechar::UInt8, escapechar::UInt8,
                        fields::BufferedVector{WeakRefString{UInt8}},
                        eager_parse_fields::Bool, pos::MutFilePos
                       )::Union{Nothing, Tuple{Bool, FilePos, WeakRefString{UInt8}, SizeType}}
    prev_pos = pos.v
    current_pos = prev_pos
    # take the pointer to buffer only once
    buff_ptr::Ptr{UInt8} = pointer(buff) + buff_start
    # cache the buffer length
    buff_len::SizeType = SizeType(length(buff))
    buff_len_remained::SizeType = buff_len - buff_start
    
    # accumulates the number of bytes read for the current line
    # (it won't count the leading whitespaces)
    num_bytes_read::SizeType = ZERO
    
    # the buffer for storing fields (if `eager_parse_fields` is true)
    fields_buff = fields.buff
    # cache the length of fields buffer
    fields_len::SizeType = SizeType(length(fields_buff))
    # accumulates the number of fields read in this line (if `eager_parse_fields` is true)
    num_fields_read::SizeType = ZERO
    # stores the index of the separator for the previous field.
    prev_field_index::SizeType = ZERO

    # `inside_quote` determines whether the given fields is quoted and we are scanning
    # inside the quote this variable is initialized here, as there might be leading
    # whitespaces inside the quote
    inside_quote::Bool = false
    
    is_escaped::Bool = false
    
    same_quote_and_escape::Bool = quotechar == escapechar
    
    # this first while-loop is for bypassing the empty lines or the lines with only whitespaces
    while num_bytes_read == ZERO && !eof(s)
        # stores the next byte value
        # it's important that in the UTF-8 format, all non-ASCII characters start with 1-bit
        # on the leftmost (in all their parts) and it makes it easy to find special characters
        # (e.g., new-line or ASCII delimiters) using the byte value.
        current_char::UInt8 = ZERO_UINT8
        
        # we look for a new-line character or end-of-file
        while !is_end_of_line_char(current_char)
            if num_bytes_read >= buff_len_remained
                return (false, FilePos(prev_pos), WeakRefString{UInt8}(buff_ptr, num_bytes_read), num_bytes_read)
            end
            
            prev_char::UInt8 = current_char
            # use a new-line character if we've already reached end of the file
            current_char = eof(s) ? ASCII_NEWLINE : Parsers.readbyte(s)
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
                if is_escaped
                    is_escaped = false
                    if !(current_char == quotechar ||  current_char == escapechar)
                        @warn("Malformed input string: escaped '\\$(Char(current_char))' is not supported.")
                    end
                elseif current_char == escapechar
                    is_escaped = true
                    continue
                elseif current_char == quotechar
                    inside_quote = !inside_quote
                    continue
                end
            end

            num_bytes_read += ONE
            if eager_parse_fields && !inside_quote && current_char == delim
                (num_fields_read, fields_len) = private_add_field(num_fields_read, fields_len,
                                                                  fields_buff, buff_ptr,
                                                                  prev_field_index, num_bytes_read)
                prev_field_index = num_bytes_read
            end
            unsafe_store!(buff_ptr + num_bytes_read - ONE, current_char)
        end
    end
    
    if num_bytes_read > ZERO
        # handle the last field if anything is read at all
        if eager_parse_fields
            
            (num_fields_read, fields_len) = private_add_field(num_fields_read, fields_len,
                                                              fields_buff, buff_ptr,
                                                              prev_field_index, num_bytes_read)
            # correctly set the number of fields read to `num_fields_read`
            fields.size = num_fields_read
        end
        pos.v = current_pos
        last_char = unsafe_load(buff_ptr, num_bytes_read - ONE)
        num_chars_in_line = !is_end_of_line_char(last_char) ? num_bytes_read - ONE : num_bytes_read
        (true, FilePos(prev_pos), WeakRefString{UInt8}(buff_ptr, num_chars_in_line), num_bytes_read)
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
- `num_rows`: the number of rows to be loaded at once. These rows will be processed
              column-at-a-time.
- `lines_buff_len`: the buffer size (in bytes) used for storing lines to be parsed
"""
function csvread(input::Union{IO,AbstractString}; delim::Union{Char, Nothing}=nothing,
                 lazy=true, eager_parse_fields=DEFAULT_EAGER_PARSE_FIELDS,
                 quotechar::Char=DEFAULT_QUOTE, escapechar::Char=quotechar,
                 header_exists::Bool=DEFAULT_HEADER_EXISTS,
                 lines_buff_len::SizeType=DEFAULT_LINE_LEN,
                 num_rows::SizeType=DEFAULT_NUM_ROWS, kw...)
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
                fields_buff_len=DEFAULT_NUM_FIELDS,
                lines_buff_len=lines_buff_len, num_rows=num_rows)
    
    lazy ? file : materialize(file)
end

export csvread

end # module
