mutable struct Counter
    v::Int
end

struct File{IO_TYPE}
    input::IO_TYPE
    delim::UInt8 # the delimiter should be an ASCII character to fit in a single byte
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
