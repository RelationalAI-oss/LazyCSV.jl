const DEFAULT_DELIM = ','
const DEFAULT_QUOTE = '"'
const DEFAULT_NUM_ROWS = SizeType(2048)
const DEFAULT_LINE_LEN = DEFAULT_NUM_ROWS * SizeType(1024)
const DEFAULT_NUM_FIELDS = SizeType(16)
const DEFAULT_EAGER_PARSE_FIELDS = true

"""
A mutable struct for keeping a counter

This struct can be used as a field of another immutable struct
"""
mutable struct Counter
    v::Int
end

# Mutable File Position
const MutFilePos = Counter

# Immutable File Position
struct FilePos
    v::Int
end

Base.hash(x::FilePos, h::UInt) = hash(x.v)
Base.isless(x::FilePos, y::FilePos) = isless(x.v, y.v)
Base.isequal(x::FilePos, y::FilePos) = x.v == y.v

const DUMMY_STRING = ""
const DUMMY_WEAK_STRING = WeakRefString{UInt8}(pointer(DUMMY_STRING),0)

"""
This struct holds all the information in a record inside a CSV file

Note: it is mutable, as it'll be re-used multiple times (once per block of records)
"""
mutable struct Record
    is_valid::Bool
    pos::FilePos
    fields_buff::BufferedVector{WeakRefString{UInt8}}
    line::WeakRefString{UInt8}
    
    function Record(fields_buff_len::SizeType) where {T}
        fields = BufferedVector{WeakRefString{UInt8}}()
        resize!(fields, fields_buff_len)
        new(true, FilePos(0), fields, DUMMY_WEAK_STRING)
    end
end

struct File{IO_TYPE}
    input::IO_TYPE    # the underlying input IO to be parsed
    delim::UInt8      # the delimiter should be an ASCII character to fit in a single byte
    quotechar::UInt8  # the quote character should be an ASCII character to fit in a single byte
    escapechar::UInt8 # the escape character should be an ASCII character to fit in a single byte
    header_exists::Bool      # does this file have a header?
    eager_parse_fields::Bool # should we parse the fields eagerly, or just parsing a line
                             # (i.e., a record) without digging into its fields is enough?
                             # This is useful for skipping records.
    lines_buff::Vector{UInt8} # A buffer that contains the line for the current record.
                             # All the `WeakRefString` instances point to a location in this
                             # `Vector`.
    recs::BufferedVector{Record} # a buffer for the fields of the current record
    current_line::Counter        # The counter that keeps the index of current line
                                 # (i.e., record) and it's used for making sure that all
                                 # iterations over this file are done sequentially.
                                 # Note that the counter value is a local value and it's not
                                 # shared between several workers in concurrent data loading.
    current_byte_pos::MutFilePos # The byte position of the current row (i.e., record)
    function File(input::IO_TYPE, delim::UInt8, quotechar::UInt8, escapechar::UInt8,
                  header_exists::Bool, eager_parse_fields::Bool, lines_buff::Vector{UInt8},
                  recs::BufferedVector{Record},
                  start_file_pos::Int64) where {IO_TYPE}
        new{IO_TYPE}(input, delim, quotechar, escapechar, header_exists,
                     eager_parse_fields, lines_buff, recs, Counter(0),
                     MutFilePos(start_file_pos))
    end
end

function File(input::IO; delim::Char=DEFAULT_DELIM,
              eager_parse_fields::Bool=DEFAULT_EAGER_PARSE_FIELDS,
              lines_buff_len::SizeType=DEFAULT_LINE_LEN, fields_buff_len::SizeType=DEFAULT_NUM_FIELDS,
              quotechar::Char=DEFAULT_QUOTE, escapechar::Char=quotechar,
              header_exists::Bool=DEFAULT_HEADER_EXISTS, start_file_pos::Int64=0,
              num_rows::SizeType=DEFAULT_NUM_ROWS)
    buff = Vector{UInt8}()
    resize!(buff, lines_buff_len)

    recs = BufferedVector{Record}()
    resize!(recs, num_rows)
    for i in 1:num_rows
        recs[i] = Record(fields_buff_len)
    end
    empty!(recs)

    File(input, UInt8(delim), UInt8(quotechar), UInt8(escapechar), header_exists,
        eager_parse_fields, buff, recs, start_file_pos)
end

function Base.iterate(f::File, state::Int = 1)
    if f.current_line.v + 1 == state
        next_pos_line = read_csv_line!(f)
        if next_pos_line === nothing
            nothing
        else
            f.current_line.v += 1
            (next_pos_line, state+1)
        end
    else
        #TODO: shall we throw an error here?
        # we are expecting the user to always scan sequentially
        nothing
    end
end

result_line_collection(f::File) = Vector{String}()
result_collection(f::File) = Vector{Vector{String}}()
first_rec(f::File) = f.recs[1]
first_rec_fields(f::File) = first_rec(f).fields_buff

function materialize_line(f::File, pos::FilePos, line)
    line_vec = result_line_collection(f)
    for field in first_rec_fields(f)
        push!(line_vec, string(field))
    end
    line_vec
end

function materialize(f::File, vec)
    counter = 0
    while (pos_line = read_csv_line!(f)) != nothing
        (pos, line) = pos_line
        push!(vec, materialize_line(f, pos, line))
        counter += 1
    end
    f.current_line.v = counter
    nothing
end

"""
A helper function for parsing and materializing the while `File` into a `Vector`
"""
function materialize(f::File)
    materialize(f, result_collection(f))
end

function num_fields_for_current_line(f::File)
    length(first_rec_fields(f))
end

"""
A helper function for counting the number of lines in a `File`
"""
function count_lines(csv_file::File)
    counter = UInt(0)
    for pos_line in csv_file
        counter += 1
    end
    counter
end

"""
A helper function for counting the number of fields in a `File`
"""
function count_fields(csv_file::File)
    counter = UInt(0)
    for pos_line in csv_file
        counter += num_fields_for_current_line(csv_file)
    end
    counter
end

"""
A helper function for converting a CSV field value to string and writing it into an output buffer
"""
function csv_field_string(buff::IO, csv_file::File, field, i, force_quote::Bool=false)
    quotechar = Char(csv_file.quotechar)
    escapechar = Char(csv_file.escapechar)
    delim = Char(csv_file.delim)
    has_delim = occursin(delim, field)
    has_quote = occursin(quotechar, field)
    if has_delim || has_quote || force_quote
        if has_quote
            field_escape_quotes = replace(field, quotechar => "$escapechar$quotechar")
            write(buff, "$quotechar$field_escape_quotes$quotechar")
        else
            write(buff, "$quotechar$field$quotechar")
        end
    else
        write(buff, field)
    end
    if i < length(first_rec_fields(csv_file))
        write(buff, csv_file.delim)
    end
end

export File, FilePos, num_fields_for_current_line, count_lines, count_fields
