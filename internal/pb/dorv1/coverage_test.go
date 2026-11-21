package dorv1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Every golden file on disk has a case, and every case has a file, so the TS
// suite cannot silently skip a fixture.
func TestGoldenFilesMatchCases(t *testing.T) {
	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	onDisk := map[string]bool{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".bin") {
			onDisk[strings.TrimSuffix(e.Name(), ".bin")] = true
		}
	}
	for _, tc := range allGoldenCases() {
		if !onDisk[tc.name] {
			t.Errorf("case %s has no golden file", tc.name)
		}
		delete(onDisk, tc.name)
	}
	for name := range onDisk {
		t.Errorf("golden file %s.bin has no case", name)
	}
}

// Every message type and every oneof arm in the schema appears in a golden
// case, and every case is fully populated (no zero-valued scalar that would
// hide a field-number mix-up).
func TestGoldenCasesCoverSchema(t *testing.T) {
	seenTypes := map[protoreflect.FullName]bool{}
	seenArms := map[protoreflect.FullName]bool{}
	var walk func(m protoreflect.Message)
	walk = func(m protoreflect.Message) {
		seenTypes[m.Descriptor().FullName()] = true
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if fd.ContainingOneof() != nil && !fd.ContainingOneof().IsSynthetic() {
				seenArms[fd.FullName()] = true
			}
			switch {
			case fd.IsMap():
				if fd.MapValue().Message() != nil {
					v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool { walk(mv.Message()); return true })
				}
			case fd.IsList() && fd.Message() != nil:
				for i := 0; i < v.List().Len(); i++ {
					walk(v.List().Get(i).Message())
				}
			case fd.Message() != nil:
				walk(v.Message())
			}
			return true
		})
	}
	var fileDesc protoreflect.FileDescriptor
	for _, tc := range allGoldenCases() {
		walk(tc.msg.ProtoReflect())
		fileDesc = tc.msg.ProtoReflect().Descriptor().ParentFile()
	}
	msgs := fileDesc.Messages()
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		if !seenTypes[md.FullName()] {
			t.Errorf("message %s is not in any golden case", md.FullName())
		}
		oneofs := md.Oneofs()
		for j := 0; j < oneofs.Len(); j++ {
			fields := oneofs.Get(j).Fields()
			for k := 0; k < fields.Len(); k++ {
				if !seenArms[fields.Get(k).FullName()] {
					t.Errorf("oneof arm %s is not in any golden case", fields.Get(k).FullName())
				}
			}
		}
	}
	// Every top-level message has all its non-oneof fields set in some case.
	setFields := map[protoreflect.FullName]bool{}
	var mark func(m protoreflect.Message)
	mark = func(m protoreflect.Message) {
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			setFields[fd.FullName()] = true
			switch {
			case fd.IsMap():
				if fd.MapValue().Message() != nil {
					v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool { mark(mv.Message()); return true })
				}
			case fd.IsList() && fd.Message() != nil:
				for i := 0; i < v.List().Len(); i++ {
					mark(v.List().Get(i).Message())
				}
			case fd.Message() != nil:
				mark(v.Message())
			}
			return true
		})
	}
	for _, tc := range allGoldenCases() {
		mark(tc.msg.ProtoReflect())
	}
	for i := 0; i < msgs.Len(); i++ {
		fields := msgs.Get(i).Fields()
		for k := 0; k < fields.Len(); k++ {
			if !setFields[fields.Get(k).FullName()] {
				t.Errorf("field %s is never populated in a golden case", fields.Get(k).FullName())
			}
		}
	}
}

// Marshalling is deterministic and the files on disk are byte-identical to a
// fresh marshal (catches a stale checked-in fixture without rewriting it).
func TestGoldenBytesAreCurrent(t *testing.T) {
	opts := proto.MarshalOptions{Deterministic: true}
	for _, tc := range allGoldenCases() {
		want, err := opts.Marshal(tc.msg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(goldenDir, tc.name+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Errorf("%s.bin is stale; run go test -run TestWriteGolden", tc.name)
		}
	}
}
