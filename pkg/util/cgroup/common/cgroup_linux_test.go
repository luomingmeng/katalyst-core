//go:build linux
// +build linux

/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/bytedance/mockey"
)

func TestParseCgroupNumaValue(t *testing.T) {
	t.Parallel()

	type args struct {
		content string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]map[int]uint64
		wantErr bool
	}{
		{
			name: "cgroupv1 format",
			args: args{
				content: `total=7587426 N0=92184 N1=21339 N2=104047 N3=7374122
file=70686 N0=5353 N1=3096 N2=12817 N3=51844
anon=7516740 N0=86831 N1=18243 N2=91230 N3=7322278
unevictable=0 N0=0 N1=0 N2=0 N3=0`,
			},
			want: map[string]map[int]uint64{
				"total": {
					0: 92184,
					1: 21339,
					2: 104047,
					3: 7374122,
				},
				"file": {
					0: 5353,
					1: 3096,
					2: 12817,
					3: 51844,
				},
				"anon": {
					0: 86831,
					1: 18243,
					2: 91230,
					3: 7322278,
				},
				"unevictable": {
					0: 0,
					1: 0,
					2: 0,
					3: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "cgroupv2 format",
			args: args{
				content: `anon N0=1629990912 N1=65225723904
file N0=1892352 N1=37441536
unevictable N0=0 N1=0`,
			},
			want: map[string]map[int]uint64{
				"anon": {
					0: 1629990912,
					1: 65225723904,
				},
				"file": {
					0: 1892352,
					1: 37441536,
				},
				"unevictable": {
					0: 0,
					1: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "wrong separator",
			args: args{
				content: `anon N0:1629990912 N1:65225723904
file N0:1892352 N1:37441536
unevictable N0:0 N1:0`,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseCgroupNumaValue(tt.args.content)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCgroupNumaValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseCgroupNumaValue() = %v, want %v", got, tt.want)
				return
			}
		})
	}
}

func TestApplyCgroupConfigs(t *testing.T) {
	cgroupPath := "test/path"
	resources := &CgroupResources{
		CpuQuota:  100000,
		CpuPeriod: 200000,
	}

	mockey.PatchConvey("cgroup v1 writes period then quota", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		var writes []cgroupConfigWrite
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(dir, file, data string) (error, bool, string) {
			So(dir, ShouldEqual, "/sys/fs/cgroup/cpu/test/path")
			writes = append(writes, cgroupConfigWrite{file: file, data: data})
			return nil, true, ""
		}).Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, resources)

		So(err, ShouldBeNil)
		So(writes, ShouldResemble, []cgroupConfigWrite{
			{file: "cpu.cfs_period_us", data: "200000"},
			{file: "cpu.cfs_quota_us", data: "100000"},
		})
	})

	mockey.PatchConvey("cgroup v1 retries period after quota when period-first gets EINVAL", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		var writes []cgroupConfigWrite
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(_, file, data string) (error, bool, string) {
			writes = append(writes, cgroupConfigWrite{file: file, data: data})
			if len(writes) == 1 {
				return syscall.EINVAL, false, ""
			}
			return nil, true, ""
		}).Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, resources)

		So(err, ShouldBeNil)
		So(writes, ShouldResemble, []cgroupConfigWrite{
			{file: "cpu.cfs_period_us", data: "200000"},
			{file: "cpu.cfs_quota_us", data: "100000"},
			{file: "cpu.cfs_period_us", data: "200000"},
		})
	})

	mockey.PatchConvey("cgroup v1 checks cancellation before retrying period", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		ctx, cancel := context.WithCancel(context.Background())
		var writes []cgroupConfigWrite
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(_, file, data string) (error, bool, string) {
			writes = append(writes, cgroupConfigWrite{file: file, data: data})
			if len(writes) == 1 {
				return syscall.EINVAL, false, ""
			}
			cancel()
			return nil, true, ""
		}).Build()

		err := ApplyCgroupConfigsWithContext(ctx, cgroupPath, resources)

		So(errors.Is(err, context.Canceled), ShouldBeTrue)
		So(writes, ShouldResemble, []cgroupConfigWrite{
			{file: "cpu.cfs_period_us", data: "200000"},
			{file: "cpu.cfs_quota_us", data: "100000"},
		})
	})

	mockey.PatchConvey("cgroup v1 preserves the retry error", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		retryErr := errors.New("retry period failed")
		writes := 0
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(_, _, _ string) (error, bool, string) {
			writes++
			switch writes {
			case 1:
				return syscall.EINVAL, false, ""
			case 3:
				return retryErr, false, ""
			default:
				return nil, true, ""
			}
		}).Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, resources)

		So(errors.Is(err, retryErr), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "cpu.cfs_period_us")
		So(writes, ShouldEqual, 3)
	})

	mockey.PatchConvey("cgroup v1 cancellation after first write prevents later writes", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		ctx, cancel := context.WithCancel(context.Background())
		writes := 0
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(_, _, _ string) (error, bool, string) {
			writes++
			cancel()
			return nil, true, ""
		}).Build()

		err := ApplyCgroupConfigsWithContext(ctx, cgroupPath, resources)

		So(errors.Is(err, context.Canceled), ShouldBeTrue)
		So(writes, ShouldEqual, 1)
	})

	mockey.PatchConvey("cgroup v2 writes the unified cpu.max file", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(true).Build()
		var dir, file, data string
		mockey.Mock(InstrumentedWriteFileIfChange).To(func(gotDir, gotFile, gotData string) (error, bool, string) {
			dir, file, data = gotDir, gotFile, gotData
			return nil, true, ""
		}).Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, resources)

		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/sys/fs/cgroup/test/path")
		So(file, ShouldEqual, "cpu.max")
		So(data, ShouldEqual, "100000 200000")
	})

	mockey.PatchConvey("write errors preserve classification", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		writeErr := errors.New("write failed")
		mockey.Mock(InstrumentedWriteFileIfChange).Return(writeErr, false, "").Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, resources)

		So(errors.Is(err, writeErr), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "cpu.cfs_period_us")
	})

	mockey.PatchConvey("paths cannot escape the cpu cgroup root", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		write := mockey.Mock(InstrumentedWriteFileIfChange).Return(nil, true, "").Build()

		err := ApplyCgroupConfigsWithContext(context.Background(), "../../../../tmp", resources)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "escapes root")
		So(write.Times(), ShouldEqual, 0)
	})

	mockey.PatchConvey("pre-canceled and nil resources perform no writes", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		write := mockey.Mock(InstrumentedWriteFileIfChange).Return(nil, true, "").Build()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		So(errors.Is(ApplyCgroupConfigsWithContext(ctx, cgroupPath, resources), context.Canceled), ShouldBeTrue)
		So(ApplyCgroupConfigsWithContext(context.Background(), cgroupPath, nil), ShouldBeNil)
		So(write.Times(), ShouldEqual, 0)
	})

	mockey.PatchConvey("legacy API applies resources with a background context", t, func() {
		mockey.Mock(CheckCgroup2UnifiedMode).Return(false).Build()
		write := mockey.Mock(InstrumentedWriteFileIfChange).Return(nil, true, "").Build()

		So(ApplyCgroupConfigs(cgroupPath, resources), ShouldBeNil)
		So(write.Times(), ShouldEqual, 2)
	})
}
