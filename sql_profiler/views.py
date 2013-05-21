from collections import defaultdict

from django import http
from django.contrib.auth.decorators import permission_required
from django.views import generic
from django.template import Context, Template

from .base import get_storage


def merge(to_merge):
    keys = set(to_merge[0].keys())
    if not keys:
        return {}
    for l in to_merge:
        assert(set(l.keys()) == keys)
    sorted_items = [sorted(l.items()) for l in to_merge]
    sorted_values = [zip(*l)[1] for l in sorted_items]
    return dict(zip(sorted(keys), zip(*sorted_values)))


class ProfileList(generic.TemplateView):
    template_name = "profiler/profile_list.html"

    def get_context_data(self, **kwargs):
        context = super(ProfileList, self).get_context_data(**kwargs)
        context['profiles'] = get_storage().get_versions()
        return context

profile_list = permission_required('profile')(ProfileList.as_view())


class ProfileKCacheGrind(generic.View):
    def get(self, request, version):
        storage = get_storage(version)
        data = merge([storage.get_time(), storage.get_count()])
        self.function_locations = self._get_functions_locations(data)
        calls = list(self._get_calls(data))
        last_times = list(self._get_last_times(data))
        self.merged_calls = self._merge_duplicates(calls)
        self.last_times_merged = self._merge_duplicates(last_times)
        raport = self._get_raport()
        return http.HttpResponse(raport)

    def _merge_duplicates(self, calls):
        result = {}
        for call, value in calls:
            if call in result:
                result[call][0] += value[0]
                result[call][1] += value[1]
            else:
                result[call] = list(value)
        return result

    def _get_last_times(self, data):
        for call, value in data.iteritems():
            yield call[-1], value

    def _get_calls(self, data):
        for call, value in data.iteritems():
            previous = call[0]
            for function in call[1:]:
                yield (previous, function), value
                previous = function


    def _get_functions_locations(self, data):
        functions = defaultdict(lambda: 99999999)
        for key, value in data.iteritems():
            for call in key:
                key = self._get_function_from_call(call)
                functions[key] = min(functions[key], self._get_line_from_call(call) - 1)
        return functions

    def _get_function_from_call(self, call):
        return call[0], call[2]

    def _get_line_from_call(self, call):
        return call[1]

    def _get_raport(self):
        result = ["events: Seconds"]
        for call, value in self.last_times_merged.iteritems():
            function = self._get_function_from_call(call)
            self._add_function_header(result, function)
            result.append("%d %d" % (self.function_locations[function], 1000*value[0]))
        for call, value in self.merged_calls.iteritems():
            function = self._get_function_from_call(call[0])
            self._add_function_header(result, function)
            calle = self._get_function_from_call(call[1])
            result.append("cfl=%s" % calle[0])
            result.append("cfn=%s" % calle[1])
            result.append("calls=%d %d" % (value[1], self.function_locations[calle]))
            result.append("%d %d" % (call[0][1], 1000*value[0]))

        return "\n".join(result)

    def _add_function_header(self, result, function):
        result.append("fl=%s" % function[0])
        result.append("fn=%s" % function[1])

profile_detail = permission_required('profile')(ProfileKCacheGrind.as_view())
