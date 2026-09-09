# Internal mechanics. The public, human-readable contract is in ManuscriptValues.jl.
using CSV, DataFrames, Printf, SHA, Statistics

const SOURCE_FILES = Dict(
    :ideology => "raw/ideology_k_gap_accounting_both_universes.csv",
    :summary => "tables/ideological_universe_comparison.csv",
    :cabinet => "raw/accounting_all_inversion_decomposition.csv",
    :periods => "raw/coalition_period_quantities.csv",
    :party => "raw/party_accounting_all_years.csv",
    :duration => "raw/observed_cabinet_duration_summary.csv",
    :size => "tables/report/party_size_cabinet_summary.csv",
    :fragmentation => "tables/report/party_fragmentation_summary.csv",
    :size_groups => "tables/report/party_size_groups.csv",
    :bridge => "tables/table_appendix_cabinet_interval_bridge.csv",
    :district => "raw/district_accounting_all_years.csv",
    :linkage => "raw/cabinet_party_set_period_linkage.csv",
    :cabinet_district => "tables/table_cabinet_district_concentration.csv",
)

"""A group selects once; its explicit field list defines every scalar/text value."""
function value_group(prefix, description, source, filters, fields;
                     domain, universe = "not_applicable", k = missing,
                     selector = :unique, members = nothing)
    (; prefix, description, source, filters, fields, domain, universe, k, selector, members)
end
# Entries are (macro suffix, source field, display rule), optionally an aggregation.
value_field(entry) = (suffix = entry[1], metric = entry[2], format = entry[3],
                      aggregation = length(entry) == 4 ? entry[4] : :identity)

function number_words(n::Integer)
    n < 0 && return "minus " * number_words(-n)
    small = ("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
             "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen",
             "seventeen", "eighteen", "nineteen")
    n < 20 && return small[n + 1]
    tens = ("twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety")
    n < 100 && return tens[n ÷ 10 - 1] * (n % 10 == 0 ? "" : "-" * small[n % 10 + 1])
    n < 1000 && return number_words(n ÷ 100) * " hundred" * (n % 100 == 0 ? "" : " " * number_words(n % 100))
    error("Unsupported prose word count: $n")
end
latex_escape(value) = replace(string(value), '\\' => raw"\textbackslash{}", '&' => raw"\&",
    '%' => raw"\%", '#' => raw"\#", '_' => raw"\_", '{' => raw"\{", '}' => raw"\}", '$' => raw"\$")

"""Prose rounds independently, unlike the existing closure-preserving table displays."""
function format_value(value, rule::Symbol)
    rule == :text && return latex_escape(value)
    rule == :integer && return string(Int(value))
    rule == :words && return number_words(Int(value))
    rule == :Words && return uppercasefirst(number_words(Int(value)))
    rule == :pct2 && return @sprintf("%.2f", 100 * Float64(value))
    rule == :pct1 && return @sprintf("%.1f", 100 * Float64(value))
    rule == :abs2 && return @sprintf("%.2f", abs(Float64(value)))
    rule == :decimal2 && return @sprintf("%.2f", Float64(value))
    rule == :decimal3 && return @sprintf("%.3f", Float64(value))
    error("Unknown manuscript display rule: $rule")
end

function load_manuscript_sources(paper_root; accounting_root = paper_root)
    # Only production CSV objects can enter this path. No TeX input is read.
    Dict(key => CSV.read(joinpath(key in (:ideology, :summary, :duration, :bridge) ? paper_root : accounting_root, path), DataFrame) for (key, path) in SOURCE_FILES)
end

getfirst(row, fields, default = missing) = begin
    for field in fields
        hasproperty(row, field) && return row[field]
    end
    default
end
membership(value) = Set(strip.(split(String(value), ',')))

function strongest_row(source, year, universe, k)
    candidates = filter(r -> r.election == year && r.ideological_universe == universe &&
                            r.k == k && r.minimal_inversion, source)
    isempty(candidates) && error("No strongest minimal inversion for $year/$universe/k=$k")
    first(sort(candidates, [:vote_share, :party_count, :coalition_id]), 1)
end

const SOURCE_DOMAINS = Dict(:ideology => "ideological", :summary => "ideological",
    :cabinet => "cabinet", :periods => "cabinet", :party => "party", :duration => "cabinet",
    :size => "party", :fragmentation => "party", :size_groups => "party", :bridge => "cabinet_bridge",
    :district => "district", :linkage => "cabinet", :cabinet_district => "cabinet")

function select_group(group, source; sources = Dict(:ideology => source))
    group.domain == SOURCE_DOMAINS[group.source] || error("$(group.prefix): source domain mismatch")
    rows = filter(source) do row
        all(isequal(row[field], value) for (field, value) in pairs(group.filters))
    end
    isempty(rows) && error("$(group.prefix): no analytical object matches $(group.filters)")
    if hasproperty(rows, :ideological_universe)
        all(isequal(group.universe), rows.ideological_universe) || error("$(group.prefix): universe mismatch")
    else
        group.universe == "not_applicable" || error("$(group.prefix): source has no universe")
    end
    if !ismissing(group.k)
        hasproperty(rows, :k) && all(isequal(group.k), rows.k) || error("$(group.prefix): k mismatch")
    end
    domain_field = hasproperty(rows, :case_domain) ? :case_domain : hasproperty(rows, :domain) ? :domain : nothing
    domain_field === nothing || all(isequal(group.domain), rows[!, domain_field]) || error("$(group.prefix): domain mismatch")
    if group.members !== nothing
        field = hasproperty(rows, :parties) ? :parties : :coalition_parties
        rows = filter(row -> membership(row[field]) == Set(group.members), rows)
    end
    if group.selector in (:strongest_span, :strongest_omission)
        year = hasproperty(group.filters, :election) ? group.filters.election : group.filters.election_year
        selected = only(eachrow(strongest_row(sources[:ideology], year, "seat_winning", 1)))
        rows = if group.selector == :strongest_span
            filter(r -> r.left_endpoint == selected.left_endpoint && r.right_endpoint == selected.right_endpoint, rows)
        else
            filter(r -> r.party == selected.omitted_party, rows)
        end
    elseif group.selector == :strongest
        # Existing production rule: minimum national vote share (equivalently maximum
        # 257-q_C), then fewest member parties, then canonical coalition_id.
        # The domain is already restricted to minimal inversions of one election/universe/k.
        all(rows.minimal_inversion) || error("Strongest selector requires minimal inversions")
        rows = first(sort(rows, [:vote_share, :party_count, :coalition_id]), 1)
    elseif group.selector != :aggregate && group.selector != :unique
        error("Unknown selector $(group.selector)")
    end
    group.selector == :aggregate || nrow(rows) == 1 || error(
        "$(group.prefix): expected exactly one fixed analytical object, found $(nrow(rows))")
    isempty(rows) && error("$(group.prefix): fixed coalition membership is absent or changed")
    rows
end

function metric_value(row, field)
    field == :label && return string(row.left_endpoint, "--", row.right_endpoint)
    field == :label_with_gap && return replace(String(row.coalition_label), ", omitting" => " omitting")
    field == :within_adjusted_quota && return row.q_C + row.A_C
    field == :includes_PT && return "PT" in membership(row.parties)
    field == :count && return 1
    row[field]
end

function extract_value(rows, field)
    values = [metric_value(row, field.metric) for row in eachrow(rows)]
    op = field.aggregation
    op == :identity && return only(values)
    op == :sum && return sum(values)
    op == :sum_minus_one && return sum(values) - 1
    op == :min && return minimum(values)
    op == :max && return maximum(values)
    op == :mean && return mean(values)
    op == :median && return median(values)
    op == :count_positive && return count(>(0), values)
    op == :count_negative && return count(<(0), values)
    op == :unique_count && return length(unique(values))
    error("Unknown manuscript aggregation: $op")
end

function exact_value(rows, field)
    field.aggregation == :identity || return missing
    row = only(eachrow(rows))
    exact = Symbol(field.metric, "_exact")
    hasproperty(row, exact) && return row[exact]
    field.metric == :vote_share && return string(BigInt(getfirst(row, (:votes, :v_C, :v_i))) // BigInt(row.V))
    missing
end

"""Select each claim once and attach its fields to the same source identity."""
function build_registry(sources; specs = MANUSCRIPT_VALUE_SPECS)
    records = NamedTuple[]
    for group in specs
        rows = select_group(group, sources[group.source]; sources)
        row = first(eachrow(rows))
        ids = [string(getfirst(r, (:case_id, :coalition_id, :cabinet_party_set_id),
                             getfirst(r, (:party, :cabinet_period, :electoral_unit), "aggregate"))) for r in eachrow(rows)]
        label = hasproperty(row, :left_endpoint) ? metric_value(row, :label) : getfirst(row, (:case_label, :party, :cabinet_period))
        identity = all(==("aggregate"), ids) ? missing : join(ids, "; ")
        rule = "$(group.selector); filters=$(group.filters); members=$(group.members)"
        group.selector in (:strongest_span, :strongest_omission) && (rule *= "; linked to seat_winning k=1 strongest minimal inversion of this election")
        group.selector == :strongest && (rule *= "; min(vote_share), party_count, canonical coalition_id")
        for entry in group.fields
            field = value_field(entry)
            raw = extract_value(rows, field)
            key = group.prefix * field.suffix
            push!(records, (; macro_name = key, semantic_key = key,
                description = "$(group.description): $(field.metric) ($(field.aggregation))",
                domain = group.domain, ideological_universe = group.universe, k = group.k,
                election = length(unique([getfirst(r, (:election, :election_year)) for r in eachrow(rows)])) == 1 ? getfirst(row, (:election, :election_year)) : missing, case_id = identity,
                case_label = nrow(rows) == 1 ? label : missing, selector_rule = rule, metric = string(field.metric),
                raw_value = string(raw), exact_value = exact_value(rows, field),
                display_value = format_value(raw, field.format), format = string(field.format),
                source_object = string(group.source), source_file = SOURCE_FILES[group.source],
                selector_source_file = group.selector in (:strongest_span, :strongest_omission) ? SOURCE_FILES[:ideology] : SOURCE_FILES[group.source],
                selection_group = group.prefix, source_row_count = nrow(rows)))
        end
    end
    registry = DataFrame(records)
    rename!(registry, :macro_name => :macro)
    validate_registry(registry)
    registry
end

function validate_registry(registry)
    for field in (:semantic_key, :macro)
        allunique(registry[!, field]) || error("Duplicate manuscript $field")
    end
    for rows in groupby(registry, :selection_group)
        length(unique(rows.case_id)) == 1 || error("Label/value source drift in $(rows.selection_group[1])")
        length(unique(rows.selector_rule)) == 1 || error("Selector drift within manuscript group")
        if startswith(first(rows.selector_rule), "strongest")
            all(rows.source_row_count .== 1) || error("Dynamic group must resolve to one row")
        end
    end
    for row in eachrow(registry)
        endswith(row.source_file, ".csv") || error("Manuscript values must not read generated TeX")
        rule = Symbol(row.format)
        raw = rule == :text ? row.raw_value : parse(Float64, row.raw_value)
        format_value(raw, rule) == row.display_value || error("Display/raw mismatch: $(row.macro)")
    end
    true
end

render_tex(registry) = join(vcat([
    "% Generated by ManuscriptValues.jl. Do not edit manually.",
    "% Source: manuscript_values.csv / production analysis objects.",
], ["\\newcommand{\\$(row.macro)}{$(row.display_value)}" for row in eachrow(registry)]), "\n") * "\n"

# TeX commands unrelated to empirical values are provided by the document class/packages
# or defined in the manuscript. Namespace scanning catches even unknown/stale value keys.
const VALUE_PREFIXES = ("Ideology", "Cabinet", "Party", "District", "Empirical", "Acct")
function manuscript_macro_uses(source)
    uncommented = replace(source, r"(?m)(?<!\\)%.*$" => "")
    Set(m.captures[1] for m in eachmatch(r"\\([A-Za-z]+)", uncommented)
        if any(startswith(m.captures[1], prefix) for prefix in VALUE_PREFIXES))
end
function validate_manuscript(registry, source)
    used = manuscript_macro_uses(source)
    stale = setdiff(used, Set(registry.macro))
    isempty(stale) || error("Undefined manuscript value macros: $(sort(collect(stale)))")
    occursin(raw"\input{manuscript_values.tex}", source) || error("Missing manuscript values import")
    !occursin("accounting_numeric_macros.tex", source) || error("Legacy macro import")
    true
end

function write_manuscript_values(output_root, registry; manuscript_source)
    validate_registry(registry)
    validate_manuscript(registry, manuscript_source)
    records = NamedTuple[]
    for (relative, kind, description) in (
        ("tables/manuscript_values.csv", "table", "Auditable scalar/text prose registry: semantic names, selectors, universe/k, raw/exact/display values and production provenance."),
        ("latex/manuscript_values.tex", "latex", "Generated manuscript prose macros serialized from the same ManuscriptValues.jl registry as manuscript_values.csv."),
    )
        path = joinpath(output_root, relative)
        mkpath(dirname(path))
        kind == "table" ? CSV.write(path, registry) : write(path, render_tex(registry))
        push!(records, (; path = relative, artifact_type = kind, description,
            rows = nrow(registry), columns = kind == "table" ? ncol(registry) : 1,
            sha256 = bytes2hex(SHA.sha256(read(path)))))
    end
    # Re-read only our CSV to check serialization; TeX is compared as output, never input.
    csv_registry = CSV.read(joinpath(output_root, "tables/manuscript_values.csv"), DataFrame;
                            types = Dict(:raw_value => String, :display_value => String))
    render_tex(csv_registry) == render_tex(registry) || error("CSV/TeX registry serialization diverged")
    records
end
