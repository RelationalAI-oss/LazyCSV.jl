using HTTP

function read_input_data(input_source::AbstractString; kwargs...)
    #TODO this should be done async
    if startswith(input_source, "https://") || startswith(input_source, "http://")
        HTTP.get(input_source; kwargs...)
    else
        open(input_source, "r")
    end
end
read_input_data(input::IOStream; kwargs...) = input

apply_mmap(input::HTTP.Messages.Response) = input.body
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
