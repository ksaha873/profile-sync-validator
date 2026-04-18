package validator

// ArtifactType captures the differences between validating user_identifiers vs
// user_traits: table-name suffixes and which inserts/deletes templates apply.
// It owns no path-level info — that's WarehouseTarget's job.
type ArtifactType interface {
	Kind() ArtifactKind
	SchemaReaderTable(spaceShortID string) string
	LoaderTable(spaceShortID string) string
	InsertsSQL(t WarehouseTarget, r ValidationRun) (string, error)
	DeletesSQL(t WarehouseTarget, r ValidationRun) (string, error)
}

func NewArtifactType(kind ArtifactKind) (ArtifactType, error) {
	switch kind {
	case ArtifactIdentifiers:
		return &identifiersArtifact{}, nil
	case ArtifactTraits:
		return &traitsArtifact{}, nil
	default:
		return nil, &SystemError{Step: "new_artifact_type", Err: errUnknownArtifact(kind)}
	}
}

type errUnknownArtifact ArtifactKind

func (e errUnknownArtifact) Error() string {
	return "unknown artifact kind: " + string(e)
}

// validationTmplData is the shared template input for inserts and deletes
// queries. Not every template field is used by every template — unused fields
// are ignored by text/template.
type validationTmplData struct {
	SpaceID           string
	Day               string
	StartAt           string
	EndAt             string
	TombstonedStartAt string
	TombstonedEndAt   string

	LoaderTable       string
	OutputPathPattern string
	DeletedCol        string
	ProfileVersionCol string
	DeletedNotTrue    string
	DataV3Table       string
}

func validationData(a ArtifactType, t WarehouseTarget, r ValidationRun) validationTmplData {
	return validationTmplData{
		SpaceID:           r.SpaceID,
		Day:               r.Day,
		StartAt:           r.StartAt,
		EndAt:             r.EndAt,
		TombstonedStartAt: r.TombstonedStartAt,
		TombstonedEndAt:   r.TombstonedEndAt,

		LoaderTable:       a.LoaderTable(r.SpaceShortID()),
		OutputPathPattern: t.OutputPathPattern(),
		DeletedCol:        t.DeletedCol(),
		ProfileVersionCol: t.ProfileVersionCol(),
		DeletedNotTrue:    t.DeletedNotTrue(),
		DataV3Table:       r.dataV3Table(),
	}
}

// --- Identifiers ---

type identifiersArtifact struct{}

func (identifiersArtifact) Kind() ArtifactKind { return ArtifactIdentifiers }

func (identifiersArtifact) SchemaReaderTable(spaceShortID string) string {
	return "schema_reader_identifiers_" + spaceShortID
}

func (identifiersArtifact) LoaderTable(spaceShortID string) string {
	return "loader_identifiers_" + spaceShortID
}

func (a identifiersArtifact) InsertsSQL(t WarehouseTarget, r ValidationRun) (string, error) {
	return renderTemplate("inserts_identifiers.sql.tmpl", validationData(a, t, r))
}

func (a identifiersArtifact) DeletesSQL(t WarehouseTarget, r ValidationRun) (string, error) {
	return renderTemplate("deletes_identifiers.sql.tmpl", validationData(a, t, r))
}

// --- Traits ---

type traitsArtifact struct{}

func (traitsArtifact) Kind() ArtifactKind { return ArtifactTraits }

func (traitsArtifact) SchemaReaderTable(spaceShortID string) string {
	return "schema_reader_traits_" + spaceShortID
}

func (traitsArtifact) LoaderTable(spaceShortID string) string {
	return "loader_traits_" + spaceShortID
}

func (a traitsArtifact) InsertsSQL(t WarehouseTarget, r ValidationRun) (string, error) {
	return renderTemplate("inserts_traits.sql.tmpl", validationData(a, t, r))
}

func (a traitsArtifact) DeletesSQL(t WarehouseTarget, r ValidationRun) (string, error) {
	return renderTemplate("deletes_traits.sql.tmpl", validationData(a, t, r))
}
