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
