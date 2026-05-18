package main

const minSegmentSize = 100

// ageGroups определяет возрастные диапазоны для сегментации.
var ageGroups = []struct {
	name  string
	descr string
	min   int
	max   int
}{
	{name: "0-17", descr: "Несовершеннолетние", min: 0, max: 17},
	{name: "18-25", descr: "Молодёжь", min: 18, max: 25},
	{name: "26-35", descr: "Молодые взрослые", min: 26, max: 35},
	{name: "36-50", descr: "Взрослые", min: 36, max: 50},
	{name: "50+", descr: "Старшая аудитория", min: 51, max: 999},
}

// segmentByAge делит клиентов на возрастные группы.
// Сегменты с количеством < minSegmentSize отбрасываются.
func segmentByAge(clients []Client) []Segment {
	buckets := make(map[string][]Client)

	for _, c := range clients {
		for _, g := range ageGroups {
			if c.Age >= g.min && c.Age <= g.max {
				buckets[g.name] = append(buckets[g.name], c)
				break
			}
		}
	}

	var segments []Segment
	for _, g := range ageGroups {
		group, ok := buckets[g.name]
		if !ok || len(group) < minSegmentSize {
			continue
		}
		segments = append(segments, Segment{
			Name:        "Возраст: " + g.name,
			Description: g.descr,
			Clients:     group,
			Count:       len(group),
		})
	}
	return segments
}

// segmentByRegion делит клиентов по региону.
// Сегменты с количеством < minSegmentSize отбрасываются.
func segmentByRegion(clients []Client) []Segment {
	buckets := make(map[string][]Client)

	for _, c := range clients {
		buckets[c.Region] = append(buckets[c.Region], c)
	}

	var segments []Segment
	for region, group := range buckets {
		if len(group) < minSegmentSize {
			continue
		}
		segments = append(segments, Segment{
			Name:        "Регион: " + region,
			Description: "Клиенты из региона " + region,
			Clients:     group,
			Count:       len(group),
		})
	}
	return segments
}

// segment выполняет полную сегментацию: по возрасту и по региону.
func segment(clients []Client) []Segment {
	segments := segmentByAge(clients)
	segments = append(segments, segmentByRegion(clients)...)
	return segments
}
